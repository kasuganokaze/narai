package com.narai.redis.subscribe;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author: kaze
 * @date: 2019-02-20
 */
@Slf4j
@Component
@EnableScheduling
public class Subscriber {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Scheduled(fixedRate = 3600000)
    public void send() {
        stringRedisTemplate.convertAndSend(RedisSubscribeConfig.REDIS_CHANNEL, "我是订阅消息");
    }

    public void handleMessage(Object message) {
        log.info("{}", message);
    }

}
