package com.openlab.rockermq.demo.basic;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;


public class SyncProducer {
    public static void main(String[] args) throws Exception {
        // 1. 创建一个 Producer 对象，指定组名称
        DefaultMQProducer producer = new DefaultMQProducer("sync_group1");

        // 2. 指定 NameServer 地址
        producer.setNamesrvAddr("192.168.6.131:9876");

        // 3. 启动消息
        producer.start();

        // 4. 指定主题和标签以及消息体
        String topic = "Sync-Topic";
        String tags = "Sync_TagA";
        byte[] body = "普通的同步消息".getBytes();
        Message msg = new Message(topic, tags, body);

        // 5. 发送消息
        SendResult send = producer.send(msg);
        System.out.printf("%s%n", send);

        TimeUnit.SECONDS.sleep(1);

        // 6. 关闭生产者对象来释放资源
        producer.shutdown();

    }
}
