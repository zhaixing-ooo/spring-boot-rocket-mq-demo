package com.openlab.listener;

import com.openlab.entity.User;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;


@Component
@RocketMQMessageListener(consumerGroup = "${rocketmq.producer.group}", topic = "user-add-topic")
public class UserAddConsumerListener implements RocketMQListener<User> {
    @Override
    public void onMessage(User user) {
        // 在 MQ 中把 user 存入到数据库中
        System.out.println("用户 " + user.getUsername() + " 添加到数据库中成功");
    }
}
