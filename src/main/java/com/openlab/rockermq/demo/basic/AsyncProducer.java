package com.openlab.rockermq.demo.basic;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

public class AsyncProducer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("async-group1");
        producer.setNamesrvAddr("192.168.6.131:9876");
        // 失败后的重试次数
        //producer.setRetryTimesWhenSendAsyncFailed(5);
        producer.start();

        String topic = "async-topic";
        String tags = "async-tagA, async-tagB";
        byte[] body = "发送异步普通消息".getBytes();
        Message message = new Message(topic, tags, body);

        producer.send(message, new SendCallback() {
            // 处理消息投递成功的操作
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("消息发送成功");
            }
            // 处理消息投递失败的操作
            @Override
            public void onException(Throwable e) {
                System.out.println("消息发送失败，失败的原因为：" + e.getMessage());
            }
        });

        // 异步发送，如果要求可靠传输，必须要等回调接口返回明确结果后才能结束逻辑，否则立即关闭Producer可能导致部分消息尚未传输成功
        TimeUnit.SECONDS.sleep(3);

        producer.shutdown();

    }
}
