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
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.BrokerRole;

/**
 * EndTransaction processor: process commit and rollback message
 */
public class EndTransactionProcessor implements NettyRequestProcessor {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);
    private final BrokerController brokerController;

    public EndTransactionProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final EndTransactionRequestHeader requestHeader =
                (EndTransactionRequestHeader) request.decodeCommandCustomHeader(EndTransactionRequestHeader.class);
        LOGGER.info("Transaction request:{}", requestHeader);
        //主备模式，主挂了不可以把slave升为主，要重启原挂掉的主，防止事务send到原主上，commit却到到新的主原slave上
        if (BrokerRole.SLAVE == brokerController.getMessageStoreConfig().getBrokerRole()) {
            response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
            LOGGER.warn("Message store is slave mode, so end transaction is forbidden. ");
            return response;
        }

        if (requestHeader.getFromTransactionCheck()) {
            //因为broker回查，所以producer发过来end请求
            switch (requestHeader.getCommitOrRollback()) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, but it's pending status.RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.toString(), request.getRemark());
                    return null;
                }
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer commit the message.RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.toString(), request.getRemark());
                    break;
                }
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("Check producer[{}] transaction state, the producer rollback the message.RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.toString(), request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        } else {
            switch (requestHeader.getCommitOrRollback()) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message,  and it's pending status.RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.toString(), request.getRemark());
                    return null;
                }
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: {
                    break;
                }
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE: {
                    LOGGER.warn("The producer[{}] end transaction in sending message, rollback the message.RequestHeader: {} Remark: {}",
                            RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.toString(), request.getRemark());
                    break;
                }
                default:
                    return null;
            }
        }
        OperationResult result = new OperationResult();
        if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == requestHeader.getCommitOrRollback()) {
            //按照phyoffset从commitLog中找到result=msg,这条msg的topic是RMQ_SYS_TRANS_HALF_TOPIC
            result = this.brokerController.getTransactionalMessageService().commitMessage(requestHeader);
            if (result.getResponseCode() == ResponseCode.SUCCESS) {
                //check 消息于请求头是否匹配
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                if (res.getCode() == ResponseCode.SUCCESS) {
                    //1）重置真正的topic信息、queueId信息put到commitLog中，回给producer success
                    MessageExtBrokerInner msgInner = endMessageTransaction(result.getPrepareMessage());
                    msgInner.setSysFlag(MessageSysFlag.resetTransactionValue(msgInner.getSysFlag(), requestHeader.getCommitOrRollback()));
                    //此处的QueueOffset没有用，往queue中放时会根据queueId重新计算，consumequeue中记录了每个queue的最大logicOffset进行加1操作就可以了
                    msgInner.setQueueOffset(requestHeader.getTranStateTableOffset());
                    msgInner.setPreparedTransactionOffset(requestHeader.getCommitLogOffset());
                    msgInner.setStoreTimestamp(result.getPrepareMessage().getStoreTimestamp());
                    //put到commitLog中
                    RemotingCommand sendResult = sendFinalMessage(msgInner);
                    if (sendResult.getCode() == ResponseCode.SUCCESS) {
                        //2）删除RMQ_SYS_TRANS_HALF_TOPIC这个topic的消息，并不是真的删除，实际上只是在commitLog中重新增加了这条消息，
                        //这条消息的topic=RMQ_SYS_TRANS_OP_HALF_TOPIC，tag是"d"（REMOVETAG），消息的内容就是RMQ_SYS_TRANS_HALF_TOPIC的消息的queueOffset（logicOffset）
                        this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                    }
                    return sendResult;
                }
                return res;
            }
        } else if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == requestHeader.getCommitOrRollback()) {
            //回滚，不同之处在于不会像commitLog put回真实的topic消息了
            //这里仅仅是查找消息，并不rollback
            result = this.brokerController.getTransactionalMessageService().rollbackMessage(requestHeader);
            if (result.getResponseCode() == ResponseCode.SUCCESS) {
                RemotingCommand res = checkPrepareMessage(result.getPrepareMessage(), requestHeader);
                if (res.getCode() == ResponseCode.SUCCESS) {
                    this.brokerController.getTransactionalMessageService().deletePrepareMessage(result.getPrepareMessage());
                }
                return res;
            }
        }
        response.setCode(result.getResponseCode());
        response.setRemark(result.getResponseRemark());
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * @param msgExt        从prepare queue中查出来的消息
     * @param requestHeader endTran请求
     */
    private RemotingCommand checkPrepareMessage(MessageExt msgExt, EndTransactionRequestHeader requestHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        if (msgExt != null) {
            final String pgroupRead = msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            //对比producer group是否匹配
            if (!pgroupRead.equals(requestHeader.getProducerGroup())) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The producer group wrong");
                return response;
            }
            //对比队列offset是否相同
            if (msgExt.getQueueOffset() != requestHeader.getTranStateTableOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The transaction state table offset wrong");
                return response;
            }
            //对比commitLog offset是否相同
            if (msgExt.getCommitLogOffset() != requestHeader.getCommitLogOffset()) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("The commit log offset wrong");
                return response;
            }
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("Find prepared transaction message failed");
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    /**
     * 更改prepare topic为real topic
     * 更改prepare queue offset为real queue offset
     *
     * @param msgExt 从prepare queue中查出来的消息
     */
    private MessageExtBrokerInner endMessageTransaction(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgInner.setQueueId(Integer.parseInt(msgExt.getUserProperty(MessageConst.PROPERTY_REAL_QUEUE_ID)));
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());
        msgInner.setWaitStoreMsgOK(false);
        msgInner.setTransactionId(msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        msgInner.setSysFlag(msgExt.getSysFlag());
        TopicFilterType topicFilterType =
                (msgInner.getSysFlag() & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG ?
                        TopicFilterType.MULTI_TAG : TopicFilterType.SINGLE_TAG;
        long tagsCodeValue = MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_TOPIC);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
        return msgInner;
    }

    private RemotingCommand sendFinalMessage(MessageExtBrokerInner msgInner) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult != null) {
            switch (putMessageResult.getPutMessageStatus()) {
                // Success
                case PUT_OK:
                case FLUSH_DISK_TIMEOUT:
                case FLUSH_SLAVE_TIMEOUT:
                case SLAVE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);
                    break;
                // Failed
                case CREATE_MAPEDFILE_FAILED:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("Create mapped file failed.");
                    break;
                case MESSAGE_ILLEGAL:
                case PROPERTIES_SIZE_EXCEEDED:
                    response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    response.setRemark("The message is illegal, maybe msg body or properties length not matched. msg body length limit 128k, msg properties length limit 32k.");
                    break;
                case SERVICE_NOT_AVAILABLE:
                    response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                    response.setRemark("Service not available now.");
                    break;
                case OS_PAGECACHE_BUSY:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("OS page cache busy, please try another machine");
                    break;
                case UNKNOWN_ERROR:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR");
                    break;
                default:
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR DEFAULT");
                    break;
            }
            return response;
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
        }
        return response;
    }
}
