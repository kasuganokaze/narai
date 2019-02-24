package com.narai.kafka.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import javax.annotation.Resource;

/**
 * @author: kaze
 * @date: 2019-02-23
 */
@Slf4j
@Component
@EnableScheduling
public class Producer {

    @Resource
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Scheduled(fixedRate = 3600000)
    public void send() {
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send("myTestTopic", "我是kafka消息");
        future.addCallback(
                result -> log.info("{}", "消息发送成功"),
                throwable -> log.info("{}", "消息发送失败")
        );
    }

}
