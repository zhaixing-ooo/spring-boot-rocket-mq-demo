package com.openlab.rockermq.demo.basic;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class SyncConsumer {
    public static void main(String[] args) throws Exception {
        // 1. 创建 Consumer 对象，并指定消费组名称，它这名称要与生产者的组名称一致
        String groupName = "async-group1";
        groupName = "one-way-group";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);

        // 2. 指定NameServer地址
        consumer.setNamesrvAddr("192.168.6.131:9876");

        // 设置消息消费的模式：集群模式和广播模式，默认是集群模式：private MessageModel messageModel = MessageModel.CLUSTERING;
        consumer.setMessageModel(MessageModel.BROADCASTING); // 指定为广播模式

        // 3. 订单主题和标签
        //String topic = "Sync-Topic";
        //String tags = "Sync_TagA";
        //String topic = "async-topic";
        //String tags = "async-tagA, async-tagB";
        String topic = "oneway-topic";
        String tags = "oneway-tag";
        consumer.subscribe(topic, tags);

        // 4. 通过回调消费消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println(msg.getQueueId() + ", " + msg.getTopic() + ", " + new String(msg.getBody()));
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 5. 启动消费者
        consumer.start();

        TimeUnit.SECONDS.sleep(2);

        // 6. 关闭消费者对象来释放资源
        consumer.shutdown();
    }
}
