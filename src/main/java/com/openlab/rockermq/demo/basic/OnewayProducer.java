package com.openlab.rockermq.demo.basic;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;


public class OnewayProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("one-way-group");
        producer.setNamesrvAddr("192.168.6.131:9876");
        producer.start();

        String topic = "oneway-topic";
        String tags = "oneway-tag";
        byte[] body = "发送单向普通消息".getBytes();
        Message message = new Message(topic, tags, body);

        producer.sendOneway(message);

        producer.shutdown();
    }
}
