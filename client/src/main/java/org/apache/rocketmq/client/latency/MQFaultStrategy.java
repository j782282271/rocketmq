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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

/**
 * broker发送失败，重试策略
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    /**
     * MQFaultStrategy中最重要的属性是latencyFaultTolerance，
     * 它维护了那些消息发送延迟较高的brokers的信息，同时延迟的时间长短对应了延迟级别latencyMax 和时长notAvailableDuration
     */
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    /**
     * sendLatencyFaultEnable 控制了是否开启延迟容错
     * 延迟容错意思为：某个broker发送超时或者失败，不立即选择其他broker，仍旧选择该broker发送消息
     * 再给该broker一段时间的延迟容错机会
     */
    private boolean sendLatencyFaultEnable = false;

    /**
     * 延迟级别数组，每次发送延迟数，都会降精度到此阶梯表中，根据该表的index索引到notAvailableDuration中
     * 判断在该次调用后，多久之后认为该broker不可用
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};

    /**
     * 不可用时长数组，多久之后认为不可用
     * 和上面对应着看
     * 如果延迟50ms或者100ms,则0秒之后就可用了，即理解可用
     * 如果延迟550ms则30000ms后才认为可用
     * 如果延迟3000ms则180000ms(180s)后才认为可用
     */
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 选择发送队列的
     * lastBrokerName：重试发送上次发送失败的BrokerName，第一次发送该值为null
     * 1.首先选择一个broker==lastBrokerName并且可用的一个队列（也就是该队列并没有因为延迟过长而被加进了延迟容错对象latencyFaultTolerance 中）
     * 2.如果第一步中没有找到合适的队列，此时舍弃broker==lastBrokerName这个条件，选择一个相对较好的broker来发送
     * 3.随机选择一个队列来发送
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        if (this.sendLatencyFaultEnable) {
            //开启了延迟容错，默认开启
            //延迟容错意思为：某个broker发送超时或者失败，不立即选择其他broker，仍旧选择该broker发送消息
            //再给该broker一段时间的延迟容错机会
            try {
                /** 1.首先选择一个broker==lastBrokerName并且可用的一个队列（也就是该队列并没有因为延迟过长而被加进了延迟容错对象latencyFaultTolerance 中）
                 * 2.如果第一步中没有找到合适的队列，此时舍弃broker==lastBrokerName这个条件，选择一个相对较好的broker来发送
                 * 3.随机选择一个队列来发送*/
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    /**延迟容错判断该broker是否可用，某个broker发送超时或者失败，不立即选择其他broker，仍旧选择该broker发送消息
                     再给该broker一段时间的延迟容错机会*/
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                if (writeQueueNums > 0) {
                    //到达此处说明lastBrokerName已完全不可用了(延迟容错也认为它不可用了)
                    //轮询下一个mq，将该mq信息置位notBestBroker
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }
        //没开启延迟容错，轮询选择一个非lastBrokerName的broker下的mq发送消息，因为上次向lastBrokerName发消息失败了，
        //所以这一次不能选该broker
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * isolation==true发送消息失败
     * currentLatency发送耗时
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 将耗时：currentLatency，降精度到指定的阶梯
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
