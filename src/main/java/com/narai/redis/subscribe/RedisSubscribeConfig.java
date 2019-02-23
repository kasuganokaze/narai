package com.narai.redis.subscribe;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;

/**
 * @author: kaze
 * @date: 2019-02-20
 */
@Configuration
public class RedisSubscribeConfig {

    public static String REDIS_CHANNEL = "channel";

    @Bean
    public RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory, MessageListenerAdapter simpleAdapter) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(simpleAdapter, new PatternTopic(REDIS_CHANNEL));
        return container;
    }

    @Bean
    public MessageListenerAdapter simpleAdapter(Subscriber consumer) {
        return new MessageListenerAdapter(consumer, "handleMessage");
    }

}
