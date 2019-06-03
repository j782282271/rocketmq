/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ClientManageProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    public ClientManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.HEART_BEAT:
                return this.heartBeat(ctx, request);
            case RequestCode.UNREGISTER_CLIENT:
                return this.unregisterClient(ctx, request);
            case RequestCode.CHECK_CLIENT_CONFIG:
                return this.checkClientConfig(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 1一个client可能既是consumer又是producer
     * 2如果心跳含有consumer信息，且broker有该consumerGroup的订阅信息则，创建重试topic
     * 3注册consumer信息
     * 4如果心跳含有producer信息，则注册producer信息
     */
    public RemotingCommand heartBeat(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody(), HeartbeatData.class);
        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(
                ctx.channel(), heartbeatData.getClientID(), request.getLanguage(), request.getVersion());

        for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
            SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().
                    findSubscriptionGroupConfig(data.getGroupName());
            boolean isNotifyConsumerIdsChangedEnable = true;
            if (null != subscriptionGroupConfig) {
                //broker端记录的subscriptionGroupConfig不为null，创建retryTopic
                isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                int topicSysFlag = 0;
                if (data.isUnitMode()) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                }
                String newTopic = MixAll.getRetryTopic(data.getGroupName());
                //创建重试topic
                this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                        newTopic, subscriptionGroupConfig.getRetryQueueNums(),
                        PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
            }

            boolean changed = this.brokerController.getConsumerManager().registerConsumer(
                    data.getGroupName(), clientChannelInfo, data.getConsumeType(), data.getMessageModel(),
                    data.getConsumeFromWhere(), data.getSubscriptionDataSet(), isNotifyConsumerIdsChangedEnable);
            if (changed) {
                log.info("registerConsumer info changed {} {}", data.toString(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            }
        }

        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            this.brokerController.getProducerManager().registerProducer(data.getGroupName(), clientChannelInfo);
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 删除producer、consumer相关信息
     */
    public RemotingCommand unregisterClient(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(UnregisterClientResponseHeader.class);
        final UnregisterClientRequestHeader requestHeader = (UnregisterClientRequestHeader) request
                .decodeCommandCustomHeader(UnregisterClientRequestHeader.class);

        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(ctx.channel(), requestHeader.getClientID(), request.getLanguage(), request.getVersion());
        {
            final String group = requestHeader.getProducerGroup();
            if (group != null) {
                this.brokerController.getProducerManager().unregisterProducer(group, clientChannelInfo);
            }
        }

        {
            final String group = requestHeader.getConsumerGroup();
            if (group != null) {
                SubscriptionGroupConfig subscriptionGroupConfig =
                        this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(group);
                boolean isNotifyConsumerIdsChangedEnable = true;
                if (null != subscriptionGroupConfig) {
                    isNotifyConsumerIdsChangedEnable = subscriptionGroupConfig.isNotifyConsumerIdsChangedEnable();
                }
                this.brokerController.getConsumerManager().unregisterConsumer(group, clientChannelInfo, isNotifyConsumerIdsChangedEnable);
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 如果请求中有SubscriptionData信息则做以下处理：
     * 如果是tag表达式订阅模式，直接返回成功code,否则
     * 如果broker没开启property过滤模式，则返回系统错误code，否则
     * 检查订阅的表达式是否满足sql格式，如果不满足则返回SUBSCRIPTION_PARSE_FAILED code，否则
     * 返回成功code
     */
    public RemotingCommand checkClientConfig(ChannelHandlerContext ctx, RemotingCommand request)
            throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        CheckClientRequestBody requestBody = CheckClientRequestBody.decode(request.getBody(), CheckClientRequestBody.class);

        if (requestBody != null && requestBody.getSubscriptionData() != null) {
            SubscriptionData subscriptionData = requestBody.getSubscriptionData();
            if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            }

            if (!this.brokerController.getBrokerConfig().isEnablePropertyFilter()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The broker does not support consumer to filter message by " + subscriptionData.getExpressionType());
                return response;
            }

            try {
                FilterFactory.INSTANCE.get(subscriptionData.getExpressionType()).compile(subscriptionData.getSubString());
            } catch (Exception e) {
                log.warn("Client {}@{} filter message, but failed to compile expression! sub={}, error={}",
                        requestBody.getClientId(), requestBody.getGroup(), requestBody.getSubscriptionData(), e.getMessage());
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark(e.getMessage());
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }
}
