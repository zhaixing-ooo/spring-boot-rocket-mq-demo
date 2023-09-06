package com.openlab.rocketmq.demo.ts;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Objects;
import java.util.concurrent.TimeUnit;


public class TransactionProducer {
    public static void main(String[] args) throws Exception {
        TransactionMQProducer producer = new TransactionMQProducer("ts-group");
        producer.setNamesrvAddr("192.168.6.131:9876");

        // 设置事务监听器
        producer.setTransactionListener(new TransactionListener() {
            // 执行本地事务
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                if (Objects.equals("tag1", msg.getTags())) {
                    return LocalTransactionState.COMMIT_MESSAGE; // 可以消费
                } else if (Objects.equals("tag2", msg.getTags())) {
                    return LocalTransactionState.ROLLBACK_MESSAGE; // 不能消费，会被删除
                } else {
                    return LocalTransactionState.UNKNOW; // 它是要回查的，只有状态变为 COMMIT后才可以消费
                }
            }

            // 回查本地事务
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

        producer.start();

        String topic = "ts-topic";
        String[] tags = { "tag1", "tag2", "tag3" };

        for (int i = 0; i < 3; i++) {
            Message message = new Message(topic, tags[i % tags.length], ("事务消息 " + tags[i]).getBytes());
            // 发送消息
            TransactionSendResult result = producer.sendMessageInTransaction(message, null);

            System.out.println(result);

            TimeUnit.SECONDS.sleep(1);

        }

    }
}
