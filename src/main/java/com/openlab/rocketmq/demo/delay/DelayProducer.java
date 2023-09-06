package com.openlab.rocketmq.demo.delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DelayProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("delay-group");
        producer.setNamesrvAddr("192.168.6.131:9876");
        producer.start();

        String topic = "dely-topic";
        String tags = "tag1";

        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Message message = new Message(topic, tags, (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date()) + " 发送延迟消息").getBytes());
            messages.add(message);
        }

        // 指定延迟时间
        //message.setDelayTimeLevel(2); // 延迟 5s 后再消费
        producer.send(messages);

        producer.shutdown();
    }
}
