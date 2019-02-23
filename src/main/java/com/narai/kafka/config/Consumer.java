package com.narai.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author: kaze
 * @date: 2019-02-23
 */
@Slf4j
@Component
public class Consumer {

    @KafkaListener(topics = {"myTestTopic"}, groupId = "myTestGroupId")
    public void receive(String message) {
        log.info("{}", message);
    }

}
