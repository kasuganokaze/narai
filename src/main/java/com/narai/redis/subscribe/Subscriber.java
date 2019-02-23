package com.narai.redis.subscribe;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author: kaze
 * @date: 2019-02-20
 */
@Slf4j
@Component
public class Subscriber {

    public void handleMessage(Object message) {
        log.info("{}", message);
    }

}
