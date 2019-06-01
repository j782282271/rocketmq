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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/***
 * 事务消息的存储
 */
public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
    }

    /**
     * RMQ_SYS_TRANS_HALF_TOPIC的queue--->RMQ_SYS_TRANS_OP_HALF_TOPIC的queue
     * prepare mq---->commit mq
     */
    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    /**
     * SendMessageProcessor，收到事务消息后将消息存入到prepare 消息队列
     * 本方法将消息真实topic放到property中，使用prepare 消息topic替换原topic,队列id为0，因为prepare 消息队列只有一个
     */
    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    /**
     * 获取msgExt中的TRANSACTION_CHECK_TIMES属性，即为broker回查producer的次数
     * 如果超过transactionCheckMax则返回true，否则增加TRANSACTION_CHECK_TIMES属性值
     */
    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            if (checkTime >= transactionCheckMax) {
                return true;
            } else {
                checkTime++;
            }
        }
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    /**
     * 消息大于消息最大保存时间(默认72小时)
     */
    private boolean needSkip(MessageExt msgExt) {
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        if (valueOfCurrentMinusBorn
                > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime() * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}", msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    /**
     * 将消息重新放入msgStore，为了不耽误offset增加
     * 重置msgExt的queueOffset、commitLogOffset、msgId
     *
     * @param offset 原offset,打日志用
     */
    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            msgExt.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());
            msgExt.setCommitLogOffset(putMessageResult.getAppendMessageResult().getWroteOffset());
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.info("Send check message, the offset={} restored in queueOffset={} commitLogOffset={} newMsgId={} realMsgId={} topic={}",
                    offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                    msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX), msgExt.getTopic());
            return true;
        } else {
            log.error("PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, msgId: {}",
                    msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    /**
     * EndTransactionProcessor.processRequest方法中处理：prepare queue的消息得到commit or rollback后，会将prepare queue的消息logicQueueOffset放到commit queue
     * 本方法的作用：定时向producer回查迟迟未得到确认的消息的状况
     * 遍历所有prepare queue，每个queue做如下处理：
     * 从最新提交的queue offset开始，依次向后+1遍历，检查每一个事务prepare消息的情况：
     * 1如果该消息已经存在于commit queue中，说明该消息不需要回查check，推进，检查下一个logQueueOffset
     * 2如果该消息超过允许的检查次数，则跳过该消息
     * 3消息的产生时间晚于开始检查时间，则停止本轮检查，直接break,检查下一个mq
     * 4如果消息没有超过免检时间，且该消息没有使用自定义免检时间则，直接break,检查下一个mq
     * 5如果消息没有超过免检时间，且该消息使用自定义免检时间则，:
     * 检查该消息是否已经重存过，如果重存过则获取其之前存储的logqueueOffset(也是初始prepare queue Offset)，看看该offset是否已经commit
     * 如果其初始prepare queue Offset已经commit，则跳过本条offset，检查下一prepare Offset
     * 如果其初始prepare queue Offset尚未commit，重存该消息到prepare queue，然后跳过本条offset，检查下一prepare Offset
     * 6如果消息超过免检时间，回查，且将消息重放如prepare queue，为了不耽误队列增加
     */
    @Override
    public void check(long transactionTimeout, int transactionCheckMax, AbstractTransactionalMessageCheckListener listener) {
        try {
            String topic = MixAll.RMQ_SYS_TRANS_HALF_TOPIC;
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.info("Check topic={}, queues={}", topic, msgQueues);

            for (MessageQueue messageQueue : msgQueues) {
                long startTime = System.currentTimeMillis();
                MessageQueue opQueue = getOpQueue(messageQueue);
                //prepare消息的已消费offset
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);
                //commit消息的已消费offset
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue, halfOffset, opOffset);
                    continue;
                }

                //多次尝试并没有得到结果，已被放到死信队列的消息，后来得到了commit/rollback，然后commit queue offset放入本list中
                List<Long> doneOpOffset = new ArrayList<>();
                //已被commit的commit queueOffset->prepare queueOffset
                HashMap<Long, Long> removeMap = new HashMap<>();
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                if (null == pullResult) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null", messageQueue, halfOffset, opOffset);
                    continue;
                }
                // single thread，记录读出prepare消息为null的次数，如果该值超过MAX_RETRY_COUNT_WHEN_HALF_NULL则停止mq循环更新offset
                int getMessageNullCount = 1;
                //从halfOffset开始检查prepare queue的消息
                long newOffset = halfOffset;
                //从halfOffset，不断加1开始遍历RMQ_SYS_TRANS_HALF_TOPIC@CID_RMQ_SYS_TRANS这个queue
                //对比removeMap、doneOpOffset哪些消息已经确认过了就可以增加maxoffset了halfOffset+n处无法确认，则将consumerOffset.json中的maxOffset置为offset+n-1
                long i = halfOffset;
                while (true) {
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    if (removeMap.containsKey(i)) {
                        //已经commit过了，可以跳过此消息++i了，即prepare mq offset可以加1了
                        log.info("Half offset {} has been committed/rolled back", i);
                        removeMap.remove(i);
                    } else {
                        //获取当前offset的消息体
                        GetResult getResult = getHalfMsg(messageQueue, i);
                        MessageExt msgExt = getResult.getMsg();
                        if (msgExt == null) {
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.info("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                        messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                        i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }

                        //检查次数过多、时间过长则丢弃该消息的检查，被丢弃的消息在后续遍历过程中才有机会出现在doneOpOffset中
                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            listener.resolveDiscardMsg(msgExt);
                            newOffset = i + 1;
                            i++;
                            continue;
                        }
                        //消息的产生时间晚于开始检查时间，则break，停止这一轮检查
                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.info("Fresh stored. the miss offset={}, check it later, store={}", i, new Date(msgExt.getStoreTimestamp()));
                            break;
                        }

                        //当前消息产生的时间
                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        long checkImmunityTime = transactionTimeout;
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        if (null != checkImmunityTimeStr) {
                            //checkImmunityTimeStr变为long，乘以1000
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                            //如果显示指定了免检时间，且当前消息的生命小于免检（免回查）时间
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                //checkPrepareQueueOffset返回true意味着，在免检时间内已经得到commit/rollback消息了，或者已重存了该消息，该消息可以跳过了，可以检查条消息了
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt, checkImmunityTime)) {
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            //如果没有显示指定免检时间，则所有消息使用统一的broker设定的免检时间，产生的时间小于免检时间则break，直接取消此轮检查，因为broker配置的免检时间比较短
                            //可以认为后面的消息都是免检的，所以break,如果消息中用户指定了免检时间，那就不一样了，因为每个消息的免检时间不同可长可短，所以需要特殊处理，见上面的分支
                            if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                log.info("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i, checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                break;
                            }
                        }
                        //从commit queue里取出来的消息,该消息只含有一些commit queue的offset信息
                        List<MessageExt> opMsg = pullResult.getMsgFoundList();
                        //判断当前获取的最后一条OpMsg的存储时间是否超过了事务超时时间，如果为true也要进行事务状态回查，为什么要这么做呢？
                        //因为在下文中，如果isNeedCheck=true，会调用putBackHalfMsgQueue重新将opMsg放入opQueue中，重新放入的消息被重置了queueOffSet，commitLogOffSet，即将消费位点前移了，放到opQueue最新一条消息中 所以
                        //如果事务状态回查成功，则fillOpRemoveMap会使得doneOpOffset包含该halfQueue offSet，即使消费位点前移了，后续也不会再重复处理
                        //如果事务状态回查失败，则判断拉取到的32条消息的最新一条消息存储时间是否超过超时时间，如果是，那肯定是回查失败的，继续进行回查
                        boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)//没有op消息，但是超过免检时间
                                //有op消息，且最后一条消息生命大于transactionTimeout，貌似op消息体的bornTimestamp，可见TransactionalMessageBridge.putOpMessage,并没有放置bornTime
                                || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout))
                                //当前消息born时间晚于检查时间(当前时间)
                                || (valueOfCurrentMinusBorn <= -1);

                        if (isNeedCheck) {
                            //把消息放回commitLog,和prepare consumerQueue,不耽误i++
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            //检查producer端该消息的状态，producer收到check消息会执行一次endTranscantion像broker发回commit/rollback消息
                            listener.resolveHalfMsg(msgExt);
                        } else {//继续取出op的消息进行check
                            pullResult = fillOpRemoveMap(removeMap, opQueue, pullResult.getNextBeginOffset(), halfOffset, doneOpOffset);
                            log.info("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}", i, messageQueue, pullResult);
                            continue;
                        }
                    }
                    newOffset = i + 1;
                    i++;
                }
                //以上所有计算都是为了更新/config/consumerOffset中的,之所以计算这么复杂是因为有可能确认的消息logicOffset不连续如果不能紧随之前的offset其后，则无法更新
                //"RMQ_SYS_TRANS_OP_HALF_TOPIC@CID_RMQ_SYS_TRANS":{0:2834}（commit/rollback消息）
                //"RMQ_SYS_TRANS_HALF_TOPIC@CID_RMQ_SYS_TRANS":{0:2936}（prepare消息）
                //如果在2937位置找到了一条消prepare消息，这条消息确认后放到commit消息处logicOffset为2836，则无法更新2834为2836，因为2835还没被确认，等到2835被确认后
                //才可以把2834-->2836
                if (newOffset != halfOffset) {
                    //更新RMQ_SYS_TRANS_HALF_TOPIC的最大offset
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    //更新RMQ_SYS_TRANS_OP_HALF_TOPIC的最大offset
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Check error", e);
        }

    }

    /**
     * 获取免检时间，checkImmunityTimeStr有Long值则使用他*1000
     * 否则返回transactionTimeout
     */
    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;
        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * Read op message, parse op message, and fill removeMap
     * 找到已经处理处的消息即已经确认过的消息(commit或rollback的消息)
     * 从consumerOffset记录的(RMQ_SYS_TRANS_OP_HALF_TOPIC@CID_RMQ_SYS_TRANS)offset开始往后找，找32条消息
     * offset往前的消息肯定是全都是已确认过的消息，因为只有offset往前的消息才会记录到consumerOffset的RMQ_SYS_TRANS_OP_HALF_TOPIC@CID_RMQ_SYS_TRANS下面
     * RMQ_SYS_TRANS_HALF_TOPIC@CID_RMQ_SYS_TRANS这个queue：  logicOffset   |1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|
     * *******************************************************commitOffset  |101|106|109|...|
     * *******************************************************CommitLog内容 |msg1|msg6|msg9|....|
     * *******************************************************consumerOffset  记录了最大消费logicOffset即下次从哪里消费：如10，意味着10以前的offset都已经处理过了commit/rollback/多次尝试并放弃
     * *RMQ_SYS_TRANS_OP_HALF_TOPIC@CID_RMQ_SYS_TRANS这个queue ：logicOffset   |1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|
     * **********************************************************commitOffset  |101|106|109|...|
     * **********************************************************CommitLog内容为RMQ_SYS_TRANS_HALF_TOPIC@CID_RMQ_SYS_TRANS的logicOffset：|2|9|11|1|4|3|6|15|.....|(造成这种非顺序的原因可以理解为有些消息迟迟得不到commit/rollback的确认，需要不断回查)
     * **********************************************************consumerOffset  记录了最大消费offset即下次从哪里消费：如3
     * 从logicOffset3开始，检查RMQ_SYS_TRANS_OP_HALF_TOPIC@CID_RMQ_SYS_TRANS的commitLog内容：如果没有则说明没有被确认过的消息，如果有，即上面的|11|1|4|3|6|15|.....
     * 这些都是被确认过多offset(commit/rollback/多次尝试并放弃)他们依次与RMQ_SYS_TRANS_HALF_TOPIC@CID_RMQ_SYS_TRANS这个queue的第10个logicOffset（也是10）比较
     * 所有的logicOffset中>=10的offset（11|15|）都认为是新来的确认(commit/rollback)消息，removeMap存11|15|即RMQ_SYS_TRANS_HALF_TOPIC@CID_RMQ_SYS_TRANS的logicOffset，需要加到文件delayOffset.json的此处："RMQ_SYS_TRANS_HALF_TOPIC@CID_RMQ_SYS_TRANS":{0:2936
     * <10的1|4|3|6（对应RMQ_SYS_TRANS_OP_HALF_TOPIC@CID_RMQ_SYS_TRANS的offset为|4|5|6|7|）都认为是：多次尝试并没有得到结果，已被放到死信队列的消息，后来得到了commit/rollback，这些offset（|4|5|6|7|）存入doneOpOffset，注意是RMQ_SYS_TRANS_OP_HALF_TOPIC@CID_RMQ_SYS_TRANS这个queue这个queue的offset
     *
     * @param removeMap      Half message to be remove, key:halfOffset(prepare offset), value: opOffset(commit offset)
     * @param opQueue        Op message queue. commit 队列
     * @param pullOffsetOfOp The begin offset of op message queue. commit 队列的开始offset
     * @param miniOffset     The current minimum offset of half message queue. prepare 队列已确认的最小offset
     * @param doneOpOffset   Stored op messages that have been processed. 多次尝试并没有得到结果，已被放到死信队列的消息，后来得到了commit/rollback
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap, MessageQueue opQueue,
                                       long pullOffsetOfOp, long miniOffset, List<Long> doneOpOffset) {
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, 32);
        if (null == pullResult) {
            return null;
        }
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }
        for (MessageExt opMessageExt : opMsg) {
            //opMessageExt：已经处理处的消息即已经确认过的消息(commit或rollback的消息)
            //已处理过消息的queueOffset logicOffset。body就是prepare队列的logic offset，见getTransactionalMessageService().deletePrepareMessage()调用，bridage.addRemoveTagInTransactionOp
            Long queueOffset = getLong(new String(opMessageExt.getBody(), TransactionalMessageUtil.charset));
            log.info("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(), opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffset);
            if (TransactionalMessageUtil.REMOVETAG.equals(opMessageExt.getTags())) {
                //miniOffset没处理过的消息即处于prepare状态的消息的最小logicOffset
                if (queueOffset < miniOffset) {
                    //存放的是RMQ_SYS_TRANS_OP_HALF_TOPIC@CID_RMQ_SYS_TRANS这个consumeQueue的offset
                    doneOpOffset.add(opMessageExt.getQueueOffset());
                } else {
                    removeMap.put(queueOffset, opMessageExt.getQueueOffset());
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }
        }
        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     * 返回true，说明该消息：已过免检时间 or 已经commit/rollback或者该消息 or 再次存储了该消息
     * 如果消息没有超过免检时间，且该消息使用自定义免检时间则:
     * 检查该消息是否已经重存过，如果重存过则获取其之前存储的logqueueOffset(也是初始prepare queue Offset)，看看该offset是否已经commit
     * 如果其初始prepare queue Offset已经commit，则返回true，外部得到true，跳过本条offset，检查下一prepare Offset
     * 如果其初始prepare queue Offset尚未commit，重存该消息到prepare queue，并返回true，外部得到true，跳过本条offset，检查下一prepare Offset
     *
     * @param removeMap         Op message map to determine whether a half message was responded by producer.
     *                          key:已经commit过的prepare消息的offset;value 已经commit过的commit消息的offset
     * @param doneOpOffset      Op Message which has been checked.
     * @param msgExt            Half message
     * @param checkImmunityTime User defined time to avoid being detected early.
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset, MessageExt msgExt, long checkImmunityTime) {
        //当前消息的生命小于免检时间
        if (System.currentTimeMillis() - msgExt.getBornTimestamp() < checkImmunityTime) {
            //当前消息在halfQueue的offset
            String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
            if (null == prepareQueueOffsetStr) {
                //如果为空，说明该消息没有被存储过免检消息，那么存储一次免检消息。存储成功返回true，失败返回false
                return putImmunityMsgBackToHalfQueue(msgExt);
            } else {
                //如果不为空，说明该消息被存储过免检消息
                long prepareQueueOffset = getLong(prepareQueueOffsetStr);
                if (-1 == prepareQueueOffset) {
                    return false;
                } else {
                    //小于免检时间，且存储过免检消息，且在最初的消息在免检时间内commit/rollback了，认为该消息通过检测了，返回true让外层i+1
                    if (removeMap.containsKey(prepareQueueOffset)) {
                        long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                        doneOpOffset.add(tmpOpOffset);
                        return true;
                    } else {
                        //小于免检时间，且存储过免检消息，在免检时间内没有得到commit/rollback信息，则将消息再次存储一遍
                        return putImmunityMsgBackToHalfQueue(msgExt);
                    }
                }
            }
        } else {
            return true;
        }
    }

    /**
     * Write messageExt to Half topic again
     * 将messageExt转换为MessageExtBrokerInner，存储到msgStore中
     * 不需要再property中保持记录第一条消息的queueOffset
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    /**
     * 将MessageExt转换为MessageExtBrokerInner
     * 且将Prepare消息的queueOffset放到MessageExtBrokerInner的属性中
     * 存储MessageExtBrokerInner
     * 同一个消息MessageExt，不管调用此方法多少遍，MessageExtBrokerInner的属性中的queueOffset都是第一条消息的queueOffset
     */
    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq     Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums   Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq     Target Message Queue
     * @param offset Offset in the message queue
     * @param nums   Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.valueOf(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.valueOf(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    /**
     * 从oldOffset开始增加offset，doneOffset连续的含有比doneOffset大的数值，有多少加多少，直到不连续为止
     */
    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;
    }

    /**
     * 根据prepare mq找到commit mq
     */
    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                    messageQueue.getQueueId());
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    /**
     * 根据logic offset获取prepare 消息
     */
    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        getResult.setPullResult(result);
        List<MessageExt> messageExts = result.getMsgFoundList();
        if (messageExts == null) {
            return getResult;
        }
        getResult.setMsg(messageExts.get(0));
        return getResult;
    }

    /**
     * 根据commitLogOffset读取消息
     */
    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    /**
     * 将prepare消息放入commit 消息队列
     */
    @Override
    public boolean deletePrepareMessage(MessageExt msgExt) {
        if (this.transactionalMessageBridge.putOpMessage(msgExt, TransactionalMessageUtil.REMOVETAG)) {
            log.info("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }

    /**
     * 实际上只是读出消息
     */
    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    /**
     * 实际上只是读出消息
     */
    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

}
