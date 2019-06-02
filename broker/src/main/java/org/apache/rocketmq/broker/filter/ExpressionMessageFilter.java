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

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageFilter;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * property sql表达式过滤+布隆过滤
 * 布隆过滤器与CommitLogDispatcherCalcBitMap类对应
 * CommitLogDispatcherCalcBitMap类在写数据到存储时，依次对比每个订阅组表达式的property sql，如果匹配上了，则为该订阅组在DispatchRequest.bitMap添加几个位的标识
 * CommitLogDispatcherCalcBitMap类外部会将该bitmap存储到ConsumeQueueExt中，这样，
 * 在consumer查消息的时候通过ExpressionMessageFilter判断ConsumeQueueExt可以快速过滤掉不满足条件的消息
 */
public class ExpressionMessageFilter implements MessageFilter {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    protected final SubscriptionData subscriptionData;
    protected final ConsumerFilterData consumerFilterData;
    protected final ConsumerFilterManager consumerFilterManager;
    /**
     * 是否使用布隆过滤器匹配，不使用的话，认为布隆过滤器匹配成功
     */
    protected final boolean bloomDataValid;

    public ExpressionMessageFilter(SubscriptionData subscriptionData, ConsumerFilterData consumerFilterData,
                                   ConsumerFilterManager consumerFilterManager) {
        this.subscriptionData = subscriptionData;
        this.consumerFilterData = consumerFilterData;
        this.consumerFilterManager = consumerFilterManager;
        if (consumerFilterData == null) {
            bloomDataValid = false;
            return;
        }
        BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
        if (bloomFilter != null && bloomFilter.isValid(consumerFilterData.getBloomFilterData())) {
            //consumerFilterData中的数据有效能够匹配上布隆过滤器，则使用布隆过滤器
            bloomDataValid = true;
        } else {
            bloomDataValid = false;
        }
    }

    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (null == subscriptionData) {
            return true;
        }
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }
        // by tags code.
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            if (tagsCode == null) {
                return true;
            }
            if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) {
                return true;
            }
            return subscriptionData.getCodeSet().contains(tagsCode.intValue());
        } else {
            // no expression or no bloom
            if (consumerFilterData == null || consumerFilterData.getExpression() == null
                    || consumerFilterData.getCompiledExpression() == null || consumerFilterData.getBloomFilterData() == null) {
                return true;
            }
            // message is before consumer
            if (cqExtUnit == null || !consumerFilterData.isMsgInLive(cqExtUnit.getMsgStoreTime())) {
                //消息存储时间早于filterData的出现时间则认为该消息无法使用布隆过滤器
                log.debug("Pull matched because not in live: {}, {}", consumerFilterData, cqExtUnit);
                return true;
            }
            byte[] filterBitMap = cqExtUnit.getFilterBitMap();
            BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
            if (filterBitMap == null || !this.bloomDataValid
                    || filterBitMap.length * Byte.SIZE != consumerFilterData.getBloomFilterData().getBitNum()) {
                return true;
            }
            BitsArray bitsArray = null;
            try {
                bitsArray = BitsArray.create(filterBitMap);
                //从cqExtUnit中计算出bitsArray，
                //依次取出BloomFilterData.bitPos 数组中的每个元素ele，判断bitsArray[ele]是否都为1，都为1则返回true
                boolean ret = bloomFilter.isHit(consumerFilterData.getBloomFilterData(), bitsArray);
                log.debug("Pull {} by bit map:{}, {}, {}", ret, consumerFilterData, bitsArray, cqExtUnit);
                return ret;
            } catch (Throwable e) {
                log.error("bloom filter error, sub=" + subscriptionData + ", filter=" + consumerFilterData + ", bitMap=" + bitsArray, e);
            }
        }
        return true;
    }

    /**
     * 上面的方法校验通过才会调用本方法
     */
    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
        if (subscriptionData == null) {
            return true;
        }
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            return true;
        }
        ConsumerFilterData realFilterData = this.consumerFilterData;
        Map<String, String> tempProperties = properties;
        // no expression
        if (realFilterData == null || realFilterData.getExpression() == null || realFilterData.getCompiledExpression() == null) {
            return true;
        }
        if (tempProperties == null && msgBuffer != null) {
            tempProperties = MessageDecoder.decodeProperties(msgBuffer);
        }

        Object ret = null;
        try {
            MessageEvaluationContext context = new MessageEvaluationContext(tempProperties);
            ret = realFilterData.getCompiledExpression().evaluate(context);
        } catch (Throwable e) {
            log.error("Message Filter error, " + realFilterData + ", " + tempProperties, e);
        }
        log.debug("Pull eval result: {}, {}, {}", ret, realFilterData, tempProperties);
        if (ret == null || !(ret instanceof Boolean)) {
            return false;
        }
        return (Boolean) ret;
    }

}
