package com.narai.redis.producer;

import com.narai.redis.config.RedisSubListenerConfig;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author: kaze
 * @date: 2019-02-20
 */
@Component
public class Producer {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void send() {
        stringRedisTemplate.convertAndSend(RedisSubListenerConfig.REDIS_CHANNEL, "haha");
    }

}
