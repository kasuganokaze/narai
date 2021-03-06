package com.narai.redis.subscribe;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author: kaze
 * @date: 2019-02-23
 */
@EnableScheduling
@Component
public class Publisher {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Scheduled(fixedRate = 3600000)
    public void send() {
        stringRedisTemplate.convertAndSend(RedisSubscribeConfig.REDIS_CHANNEL, "我是redis订阅消息");
    }

}
