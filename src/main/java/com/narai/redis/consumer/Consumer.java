package com.narai.redis.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author: kaze
 * @date: 2019-02-20
 */
@Slf4j
@Component
public class Consumer {

    public void receiver(String message) {
        log.info(message);
    }
}
