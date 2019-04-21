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
package org.apache.rocketmq.common.consumer;

/**
 * 这个参数只对一个新的consumeGroup第一次启动时有效。
 * 就是说，如果是一个consumerGroup重启，他只会从自己上次消费到的offset，继续消费。这个参数是没用的。 而判断是不是一个新的ConsumerGroup是在broker端判断。
 * 要知道，消费到哪个offset最先是存在Consumer本地的，定时和broker同步自己的消费offset。broker在判断是不是一个新的consumergroup，就是查broker端有没有这个consumergroup的offset记录。
 * 以上所说的第一次启动是指从来没有消费过的消费者，如果该消费者消费过，那么会在broker端记录该消费者的消费位置，如果该消费者挂了再启动，那么自动从上次消费的进度开始，见RemoteBrokerOffsetStore
 */
public enum ConsumeFromWhere {
    CONSUME_FROM_LAST_OFFSET,
    @Deprecated
    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
    @Deprecated
    CONSUME_FROM_MIN_OFFSET,
    @Deprecated
    CONSUME_FROM_MAX_OFFSET,
    CONSUME_FROM_FIRST_OFFSET,
    CONSUME_FROM_TIMESTAMP,
}
