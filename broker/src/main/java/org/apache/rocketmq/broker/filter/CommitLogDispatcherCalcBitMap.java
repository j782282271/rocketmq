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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.CommitLogDispatcher;
import org.apache.rocketmq.store.DispatchRequest;

import java.util.Collection;
import java.util.Iterator;

/**
 * Calculate bit map of filter.
 * 布隆过滤器与ExpressionMessageFilter类对应
 */
public class CommitLogDispatcherCalcBitMap implements CommitLogDispatcher {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    protected final BrokerConfig brokerConfig;
    protected final ConsumerFilterManager consumerFilterManager;

    public CommitLogDispatcherCalcBitMap(BrokerConfig brokerConfig, ConsumerFilterManager consumerFilterManager) {
        this.brokerConfig = brokerConfig;
        this.consumerFilterManager = consumerFilterManager;
    }

    /**
     * producer发送消息时候，异步创建消息存储到ConsumeQueueExt时候，需要此类创建bitmap
     * 1获取所有本topic的消费者相关的ConsumerFilterData集合
     * 2ConsumerFilterData中含有消费组的订阅表达式CompiledExpression
     * 3遍历所有本topic下的ConsumerFilterData集合，分别校验每个消费组是否的订阅表达式和当前消息的property是否匹配
     * 4如果匹配则，在filterBitMap，指定的几个位置记录1，之所以是几个，是因为是布隆过滤器，在这里不同的consumer group的设置为1的位置不相同
     * 这样ExpressionMessageFilter就可以区分consumer group,进行过滤消息了
     * 此处类相当于在消费时的过滤提前到发送时候异步过滤（property转bit过滤），然后放到bitmap中存储了区分信息，在过拉取消息过滤时候，优先使用该bitmap过滤，提高了性能
     */
    @Override
    public void dispatch(DispatchRequest request) {
        if (!this.brokerConfig.isEnableCalcFilterBitMap()) {
            return;
        }
        try {
            Collection<ConsumerFilterData> filterDatas = consumerFilterManager.get(request.getTopic());
            if (filterDatas == null || filterDatas.isEmpty()) {
                return;
            }
            Iterator<ConsumerFilterData> iterator = filterDatas.iterator();
            BitsArray filterBitMap = BitsArray.create(this.consumerFilterManager.getBloomFilter().getM());
            long startTime = System.currentTimeMillis();
            while (iterator.hasNext()) {
                ConsumerFilterData filterData = iterator.next();
                if (filterData.getCompiledExpression() == null) {
                    log.error("[BUG] Consumer in filter manager has no compiled expression! {}", filterData);
                    continue;
                }
                if (filterData.getBloomFilterData() == null) {
                    log.error("[BUG] Consumer in filter manager has no bloom data! {}", filterData);
                    continue;
                }
                Object ret = null;
                try {
                    MessageEvaluationContext context = new MessageEvaluationContext(request.getPropertiesMap());
                    ret = filterData.getCompiledExpression().evaluate(context);
                } catch (Throwable e) {
                    log.error("Calc filter bit map error!commitLogOffset={}, consumer={}, {}", request.getCommitLogOffset(), filterData, e);
                }
                log.debug("Result of Calc bit map:ret={}, data={}, props={}, offset={}", ret, filterData, request.getPropertiesMap(), request.getCommitLogOffset());

                // eval true
                if (ret != null && ret instanceof Boolean && (Boolean) ret) {
                    //如：BloomFilterData.bitPos=[1,5,7]，则将filterBitMap的1、5、7位置置位1
                    consumerFilterManager.getBloomFilter().hashTo(filterData.getBloomFilterData(), filterBitMap);
                }
            }
            request.setBitMap(filterBitMap.bytes());
            long eclipseTime = System.currentTimeMillis() - startTime;
            // 1ms
            if (eclipseTime >= 1) {
                log.warn("Spend {} ms to calc bit map, consumerNum={}, topic={}", eclipseTime, filterDatas.size(), request.getTopic());
            }
        } catch (Throwable e) {
            log.error("Calc bit map error! topic={}, offset={}, queueId={}, {}", request.getTopic(), request.getCommitLogOffset(), request.getQueueId(), e);
        }
    }
}
