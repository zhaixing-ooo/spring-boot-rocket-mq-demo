package com.openlab.controller;

import com.openlab.entity.User;
import jakarta.annotation.Resource;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UserController {
    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @GetMapping("/add/{id}/{name}")
    public User add(@PathVariable("name") String name, @PathVariable("id") Long id) {
        User user = new User();
        user.setId(id);
        user.setUsername(name);
        // 把 user 存入到 redis 中。
        //redisTemplate.opsForValue().set("user", user);
        // 同时向 MQ 发送消息
        rocketMQTemplate.convertAndSend("user-add-topic", user);

        return user;
    }
}
