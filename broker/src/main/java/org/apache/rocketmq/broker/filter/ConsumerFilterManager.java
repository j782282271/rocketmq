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

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.filter.FilterFactory;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.filter.util.BloomFilterData;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Consumer filter data manager.Just manage the consumers use expression filter.
 * 管理了topic->consumerGroup->ConsumerFilterData的映射关系
 * 其中ConsumerFilterData中的数据，在本类中计算，ConsumerFilterData主要包括以下数据：
 * 1property sql 表达式
 * 2topic+consumerGroup这个String，通过bloomFilter.k个函数，计算出bloomFilter.m这些位中哪几位为1，放在BloomFilterData中
 * producer发送消息时（CommitLogDispatcherCalcBitMap），遍历该topic下各个consumerGroup的BloomFilterData，如果与msg property匹配上了，为该消息bitmap对应位打标识(bitmap会被打上各个匹配上的consumerGroup的标识)
 * 该ConsumerFilterData也可用于consumer消费消息，初步过滤消息；
 */
public class ConsumerFilterManager extends ConfigManager {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    private static final long MS_24_HOUR = 24 * 3600 * 1000;

    private ConcurrentMap<String/*Topic*/, FilterDataMapByTopic> filterDataByTopic = new ConcurrentHashMap<>(256);

    private transient BrokerController brokerController;
    private transient BloomFilter bloomFilter;

    public ConsumerFilterManager() {
        // just for test
        this.bloomFilter = BloomFilter.createByFn(20, 64);
    }

    public ConsumerFilterManager(BrokerController brokerController) {
        this.brokerController = brokerController;
        this.bloomFilter = BloomFilter.createByFn(
                brokerController.getBrokerConfig().getMaxErrorRateOfBloomFilter(),
                brokerController.getBrokerConfig().getExpectConsumerNumUseFilter());
        // then set bit map length of store config.
        brokerController.getMessageStoreConfig().setBitMapLengthConsumeQueueExt(this.bloomFilter.getM());
    }

    /**
     * Build consumer filter data.Be care, bloom filter data is not included.
     * 创建ConsumerFilterData，并编译表达式expression
     *
     * @return maybe null
     */
    public static ConsumerFilterData build(final String topic, final String consumerGroup,
                                           final String expression, final String type, final long clientVersion) {
        if (ExpressionType.isTagType(type)) {
            return null;
        }
        ConsumerFilterData consumerFilterData = new ConsumerFilterData();
        consumerFilterData.setTopic(topic);
        consumerFilterData.setConsumerGroup(consumerGroup);
        consumerFilterData.setBornTime(System.currentTimeMillis());
        consumerFilterData.setDeadTime(0);
        consumerFilterData.setExpression(expression);
        consumerFilterData.setExpressionType(type);
        consumerFilterData.setClientVersion(clientVersion);
        try {
            consumerFilterData.setCompiledExpression(FilterFactory.INSTANCE.get(type).compile(expression));
        } catch (Throwable e) {
            log.error("parse error: expr={}, topic={}, group={}, error={}", expression, topic, consumerGroup, e.getMessage());
            return null;
        }
        return consumerFilterData;
    }

    /**
     * 同一个consumerGroup应该只有一个SubscriptionData，但是这里是集合，是因为
     * SubscriptionData中含有topic信息，也就是一个consumerGroup可以订阅多个topic，也即多个topic的订阅情况
     * SubscriptionData.subVersion是一个版本，所以一个consumerGroup可有多个SubscriptionData版本
     * 本方法作用：
     * 根据SubscriptionData向this.filterDataByTopic注册topic-consumerGroup-ConsumerFilterData映射map，并删除不存在的topic
     */
    public void register(final String consumerGroup, final Collection<SubscriptionData> subList) {
        for (SubscriptionData subscriptionData : subList) {
            register(subscriptionData.getTopic(), consumerGroup, subscriptionData.getSubString(),
                    subscriptionData.getExpressionType(), subscriptionData.getSubVersion());
        }
        //如果当前consumerGroup，在broker端注册了a topic,当前的订阅topic信息（subList）不含有a topic
        // 则将consumerGroup的a topic的filterData设置为dead,即删除本consumerGroup的broker端的无用topic
        Collection<ConsumerFilterData> groupFilterData = getByGroup(consumerGroup);
        Iterator<ConsumerFilterData> iterator = groupFilterData.iterator();
        while (iterator.hasNext()) {
            ConsumerFilterData filterData = iterator.next();
            boolean exist = false;
            for (SubscriptionData subscriptionData : subList) {
                if (subscriptionData.getTopic().equals(filterData.getTopic())) {
                    exist = true;
                    break;
                }
            }

            if (!exist && !filterData.isDead()) {
                filterData.setDeadTime(System.currentTimeMillis());
                log.info("Consumer filter changed: {}, make illegal topic dead:{}", consumerGroup, filterData);
            }
        }
    }

    /**
     * 1创建topic到consumerGroup的映射再到BloomFilterData的映射
     * 2consumerGroup + "#" + topic作为str，放到bloomFilter中的bloomFilter.k个hash函数计算0-bloomFilter.m位哪几位为1
     * 计算结果放到BloomFilterData.bitPos中
     */
    public boolean register(final String topic, final String consumerGroup, final String expression,
                            final String type, final long clientVersion) {
        if (ExpressionType.isTagType(type)) {
            return false;
        }
        if (expression == null || expression.length() == 0) {
            return false;
        }
        FilterDataMapByTopic filterDataMapByTopic = this.filterDataByTopic.get(topic);
        if (filterDataMapByTopic == null) {
            FilterDataMapByTopic temp = new FilterDataMapByTopic(topic);
            FilterDataMapByTopic prev = this.filterDataByTopic.putIfAbsent(topic, temp);
            filterDataMapByTopic = prev != null ? prev : temp;
        }
        BloomFilterData bloomFilterData = bloomFilter.generate(consumerGroup + "#" + topic);
        return filterDataMapByTopic.register(consumerGroup, expression, type, bloomFilterData, clientVersion);
    }

    public void unRegister(final String consumerGroup) {
        for (String topic : filterDataByTopic.keySet()) {
            this.filterDataByTopic.get(topic).unRegister(consumerGroup);
        }
    }

    public ConsumerFilterData get(final String topic, final String consumerGroup) {
        if (!this.filterDataByTopic.containsKey(topic)) {
            return null;
        }
        if (this.filterDataByTopic.get(topic).getGroupFilterData().isEmpty()) {
            return null;
        }

        return this.filterDataByTopic.get(topic).getGroupFilterData().get(consumerGroup);
    }

    /**
     * 获取一个consumerGroup下的各个topic的ConsumerFilterData
     */
    public Collection<ConsumerFilterData> getByGroup(final String consumerGroup) {
        Collection<ConsumerFilterData> ret = new HashSet<>();
        Iterator<FilterDataMapByTopic> topicIterator = this.filterDataByTopic.values().iterator();
        while (topicIterator.hasNext()) {
            FilterDataMapByTopic filterDataMapByTopic = topicIterator.next();
            Iterator<ConsumerFilterData> filterDataIterator = filterDataMapByTopic.getGroupFilterData().values().iterator();
            while (filterDataIterator.hasNext()) {
                ConsumerFilterData filterData = filterDataIterator.next();
                if (filterData.getConsumerGroup().equals(consumerGroup)) {
                    ret.add(filterData);
                }
            }
        }
        return ret;
    }

    public final Collection<ConsumerFilterData> get(final String topic) {
        if (!this.filterDataByTopic.containsKey(topic)) {
            return null;
        }
        if (this.filterDataByTopic.get(topic).getGroupFilterData().isEmpty()) {
            return null;
        }
        return this.filterDataByTopic.get(topic).getGroupFilterData().values();
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    @Override
    public String encode() {
        return encode(false);
    }

    @Override
    public String configFilePath() {
        if (this.brokerController != null) {
            return BrokerPathConfigHelper.getConsumerFilterPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        }
        return BrokerPathConfigHelper.getConsumerFilterPath("./unit_test");
    }

    /**
     * 从磁盘读取的json，需要解析到内存
     */
    @Override
    public void decode(final String jsonString) {
        ConsumerFilterManager load = RemotingSerializable.fromJson(jsonString, ConsumerFilterManager.class);
        if (load != null && load.filterDataByTopic != null) {
            boolean bloomChanged = false;
            for (String topic : load.filterDataByTopic.keySet()) {
                FilterDataMapByTopic dataMapByTopic = load.filterDataByTopic.get(topic);
                if (dataMapByTopic == null) {
                    continue;
                }

                for (String group : dataMapByTopic.getGroupFilterData().keySet()) {
                    ConsumerFilterData filterData = dataMapByTopic.getGroupFilterData().get(group);
                    if (filterData == null) {
                        continue;
                    }
                    try {
                        filterData.setCompiledExpression(FilterFactory.INSTANCE.get(filterData.getExpressionType()).compile(filterData.getExpression()));
                    } catch (Exception e) {
                        log.error("load filter data error, " + filterData, e);
                    }

                    // check whether bloom filter is changed
                    // if changed, ignore the bit map calculated before.
                    //检查 bloom filter是否改变，如果改变了，则忽略load操作，仍使用内存的
                    if (!this.bloomFilter.isValid(filterData.getBloomFilterData())) {
                        bloomChanged = true;
                        log.info("Bloom filter is changed!So ignore all filter data persisted! {}, {}", this.bloomFilter, filterData.getBloomFilterData());
                        break;
                    }
                    log.info("load exist consumer filter data: {}", filterData);
                    if (filterData.getDeadTime() == 0) {
                        //如果磁盘上的filterData没有deadTime，设置一个过期时间，设置规则：
                        //过去30s的时刻，如果小于磁盘上filterData的出生时间则使用filterData的出生时间作为其deadTime,否则使用过去30s的时间作为其deadTime
                        //即从磁盘上取出的filterData,强制让其dead
                        // we think all consumers are dead when load
                        long deadTime = System.currentTimeMillis() - 30 * 1000;
                        filterData.setDeadTime(deadTime <= filterData.getBornTime() ? filterData.getBornTime() : deadTime);
                    }
                }
            }

            if (!bloomChanged) {
                this.filterDataByTopic = load.filterDataByTopic;
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        // clean
        clean();
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    /**
     * ConsumerFilterData dead 超过24小时，就被remove掉
     */
    public void clean() {
        Iterator<Map.Entry<String, FilterDataMapByTopic>> topicIterator = this.filterDataByTopic.entrySet().iterator();
        while (topicIterator.hasNext()) {
            Map.Entry<String, FilterDataMapByTopic> filterDataMapByTopic = topicIterator.next();

            Iterator<Map.Entry<String, ConsumerFilterData>> filterDataIterator
                    = filterDataMapByTopic.getValue().getGroupFilterData().entrySet().iterator();

            while (filterDataIterator.hasNext()) {
                Map.Entry<String, ConsumerFilterData> filterDataByGroup = filterDataIterator.next();

                ConsumerFilterData filterData = filterDataByGroup.getValue();
                //dead 超过24小时，就不再持久化存储了
                if (filterData.howLongAfterDeath() >= (this.brokerController == null ? MS_24_HOUR : this.brokerController.getBrokerConfig().getFilterDataCleanTimeSpan())) {
                    log.info("Remove filter consumer {}, died too long!", filterDataByGroup.getValue());
                    filterDataIterator.remove();
                }
            }

            if (filterDataMapByTopic.getValue().getGroupFilterData().isEmpty()) {
                log.info("Topic has no consumer, remove it! {}", filterDataMapByTopic.getKey());
                topicIterator.remove();
            }
        }
    }

    public ConcurrentMap<String, FilterDataMapByTopic> getFilterDataByTopic() {
        return filterDataByTopic;
    }

    public void setFilterDataByTopic(final ConcurrentHashMap<String, FilterDataMapByTopic> filterDataByTopic) {
        this.filterDataByTopic = filterDataByTopic;
    }

    public static class FilterDataMapByTopic {

        private ConcurrentMap<String/*consumer group*/, ConsumerFilterData> groupFilterData = new ConcurrentHashMap<>();

        private String topic;

        public FilterDataMapByTopic() {
        }

        public FilterDataMapByTopic(String topic) {
            this.topic = topic;
        }

        /**
         * 将consumerGroup对应的groupFilterData设置为dead
         */
        public void unRegister(String consumerGroup) {
            if (!this.groupFilterData.containsKey(consumerGroup)) {
                return;
            }
            ConsumerFilterData data = this.groupFilterData.get(consumerGroup);
            if (data == null || data.isDead()) {
                return;
            }
            long now = System.currentTimeMillis();
            log.info("Unregister consumer filter: {}, deadTime: {}", data, now);
            data.setDeadTime(now);
        }

        /**
         * 1创建consumerGroup到ConsumerFilterData的映射，存到this.groupFilterData中
         * 2如果groupFilterData已存在该映射关系，则比较版本
         * 新来的版本大于已存在的版本，则覆盖，
         * 新来的版本等于已存在的版本，如果老版本已dead，则复活
         * 新来的版本小于已存在的版本，则忽略
         */
        public boolean register(String consumerGroup, String expression, String type, BloomFilterData bloomFilterData, long clientVersion) {
            ConsumerFilterData old = this.groupFilterData.get(consumerGroup);
            if (old == null) {
                ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                if (consumerFilterData == null) {
                    return false;
                }
                consumerFilterData.setBloomFilterData(bloomFilterData);

                old = this.groupFilterData.putIfAbsent(consumerGroup, consumerFilterData);
                if (old == null) {
                    log.info("New consumer filter registered: {}", consumerFilterData);
                    return true;
                } else {
                    if (clientVersion <= old.getClientVersion()) {
                        if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                            log.warn("Ignore consumer({} : {}) filter(concurrent), because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}",
                                    consumerGroup, topic, clientVersion, old.getClientVersion(),
                                    old.getExpressionType(), old.getExpression(), type, expression);
                        }
                        if (clientVersion == old.getClientVersion() && old.isDead()) {
                            reAlive(old);
                            return true;
                        }
                        return false;
                    } else {
                        this.groupFilterData.put(consumerGroup, consumerFilterData);
                        log.info("New consumer filter registered(concurrent): {}, old: {}", consumerFilterData, old);
                        return true;
                    }
                }
            } else {
                if (clientVersion <= old.getClientVersion()) {
                    if (!type.equals(old.getExpressionType()) || !expression.equals(old.getExpression())) {
                        log.info("Ignore consumer({}:{}) filter, because of version {} <= {}, but maybe info changed!old={}:{}, ignored={}:{}",
                                consumerGroup, topic, clientVersion, old.getClientVersion(),
                                old.getExpressionType(), old.getExpression(), type, expression);
                    }
                    if (clientVersion == old.getClientVersion() && old.isDead()) {
                        reAlive(old);
                        return true;
                    }
                    return false;
                }

                boolean change = !old.getExpression().equals(expression) || !old.getExpressionType().equals(type);
                if (old.getBloomFilterData() == null && bloomFilterData != null) {
                    change = true;
                }
                if (old.getBloomFilterData() != null && !old.getBloomFilterData().equals(bloomFilterData)) {
                    change = true;
                }

                // if subscribe data is changed, or consumer is died too long.
                if (change) {
                    ConsumerFilterData consumerFilterData = build(topic, consumerGroup, expression, type, clientVersion);
                    if (consumerFilterData == null) {
                        // new expression compile error, remove old, let client report error.
                        this.groupFilterData.remove(consumerGroup);
                        return false;
                    }
                    consumerFilterData.setBloomFilterData(bloomFilterData);
                    this.groupFilterData.put(consumerGroup, consumerFilterData);
                    log.info("Consumer filter info change, old: {}, new: {}, change: {}", old, consumerFilterData, change);
                    return true;
                } else {
                    old.setClientVersion(clientVersion);
                    if (old.isDead()) {
                        reAlive(old);
                    }
                    return true;
                }
            }
        }

        protected void reAlive(ConsumerFilterData filterData) {
            long oldDeadTime = filterData.getDeadTime();
            filterData.setDeadTime(0);
            log.info("Re alive consumer filter: {}, oldDeadTime: {}", filterData, oldDeadTime);
        }

        public final ConsumerFilterData get(String consumerGroup) {
            return this.groupFilterData.get(consumerGroup);
        }

        public final ConcurrentMap<String, ConsumerFilterData> getGroupFilterData() {
            return this.groupFilterData;
        }

        public void setGroupFilterData(final ConcurrentHashMap<String, ConsumerFilterData> groupFilterData) {
            this.groupFilterData = groupFilterData;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(final String topic) {
            this.topic = topic;
        }
    }
}
