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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.rocketmq.filter.expression.Expression;
import org.apache.rocketmq.filter.util.BloomFilterData;

import java.util.Collections;

/**
 * ConsumerFilterData主要包括以下数据：
 * 1property sql 表达式
 * 2topic+consumerGroup这个String，通过bloomFilter.k个函数，计算出bloomFilter.m这些位中哪几位为1，放在BloomFilterData中
 * 本类作用：
 * 1producer发送消息时（CommitLogDispatcherCalcBitMap），遍历该topic下各个consumerGroup，假如遍历到当前ConsumerFilterData所属consumerGroup，那么做以下处理：
 * 如果expression与msg property匹配上了，且this.bloomFilterData.bitPos[]=[1,6,43,55]，将该消息bitmap的的这几位1,6,43,55置为1，这样，遍历bitmap会被打上各个匹配上的consumerGroup的标识
 * 2该ConsumerFilterData也可用于consumer消费消息，consumer根据当前topic、consumerGroup到ConsumerFilterManager类中找到本类，然后取出this.bloomFilterData（bitPos[]=[1,6,43,55]），
 * 对比取出的消息的bitmap在1,6,43,55这几位是否都为1，如果有以为不为1，则认为该消息不匹配本consumer
 */
public class ConsumerFilterData {

    private String consumerGroup;
    private String topic;
    private String expression;
    private String expressionType;
    private transient Expression compiledExpression;
    private long bornTime;
    private long deadTime = 0;
    private BloomFilterData bloomFilterData;
    private long clientVersion;

    public boolean isDead() {
        return this.deadTime >= this.bornTime;
    }

    public long howLongAfterDeath() {
        if (isDead()) {
            return System.currentTimeMillis() - getDeadTime();
        }
        return -1;
    }

    /**
     * Check this filter data has been used to calculate bit map when msg was stored in server.
     */
    public boolean isMsgInLive(long msgStoreTime) {
        //消息存储时间晚于当前filterData的出现时间才认为该消息可以使用布隆过滤器
        return msgStoreTime > getBornTime();
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(final String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(final String expression) {
        this.expression = expression;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(final String expressionType) {
        this.expressionType = expressionType;
    }

    public Expression getCompiledExpression() {
        return compiledExpression;
    }

    public void setCompiledExpression(final Expression compiledExpression) {
        this.compiledExpression = compiledExpression;
    }

    public long getBornTime() {
        return bornTime;
    }

    public void setBornTime(final long bornTime) {
        this.bornTime = bornTime;
    }

    public long getDeadTime() {
        return deadTime;
    }

    public void setDeadTime(final long deadTime) {
        this.deadTime = deadTime;
    }

    public BloomFilterData getBloomFilterData() {
        return bloomFilterData;
    }

    public void setBloomFilterData(final BloomFilterData bloomFilterData) {
        this.bloomFilterData = bloomFilterData;
    }

    public long getClientVersion() {
        return clientVersion;
    }

    public void setClientVersion(long clientVersion) {
        this.clientVersion = clientVersion;
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o, Collections.<String>emptyList());
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, Collections.<String>emptyList());
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
