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
package org.apache.rocketmq.client.consumer.rebalance;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.ArrayList;
import java.util.List;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * RebalanceImpl.rebalanceByTopic调用本方法
     * 进来之前，RebalanceImpl.rebalanceByTopic会对mqAll、cidAll排序
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}", consumerGroup, currentCID, cidAll);
            return result;
        }

        /**
         * 1最简单理解，如果mqAll.size() <= cidAll.size(),每个cid最多分一个mq，排在前面的优先分，后面的有可能分不到
         * 如果mqAll.size() > cidAll.size()，每个cid最少分mqAll.size() / cidAll.size()个mq，令mod = mqAll.size() % cidAll.size();
         * cid list中前mod个cid可以再多分一个mq，即前mod个cid分得mqAll.size() / cidAll.size()+1g个mq，大于mod的cid，分得mqAll.size() / cidAll.size()个mq
         *
         * 2复杂理解：
         * 如果以下
         * currentCID=1
         * cidAll  c0  c1  c2
         * mqAll   q0  q1  q2  q3  q4  q5  q6  q7
         * 则index=1、mod=2、mqAll.size()/cidAll.size()=8/3=2
         * averageSize=3、startIndex=3、range=3
         * 分配结果：
         * cidAll  c0  c0  c0  c1  c1  c1  c2  c2
         * mqAll   q0  q1  q2  q3  q4  q5  q6  q7
         * */
        int index = cidAll.indexOf(currentCID);
        int mod = mqAll.size() % cidAll.size();
        int averageSize = mqAll.size() <= cidAll.size() ? 1 ://如果mqAll<=cidAll数量，则averageSize=1
                (mod > 0 && index < mod ?//如果mqAll>cidAll数量，且有mqAll对cidAll求余有余，且index小于余数，之所以不是<=是因为index为下标，mod为数量，下标从0开始
                        mqAll.size() / cidAll.size() + 1 : //averageSize= mqAll.size() / cidAll.size() + 1
                        mqAll.size() / cidAll.size()//averageSize= mqAll.size() / cidAll.size()
                );
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
