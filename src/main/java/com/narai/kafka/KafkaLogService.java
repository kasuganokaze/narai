package com.narai.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: kaze
 * @date: 2019-02-20
 */
@Slf4j
@Service
public class KafkaLogService {

    @Resource
    private ConsumerFactory<String, String> consumerFactory;

    public Integer lagOffset(String topicName) {
        try {
            Consumer<String, String> consumer = getConsumer(topicName);
            List<TopicPartition> topicPartitions = new ArrayList<>();
            List<PartitionInfo> partitionsFor = consumer.partitionsFor(topicName);
            for (PartitionInfo partitionInfo : partitionsFor) {
                TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                topicPartitions.add(topicPartition);
            }
            //查询log size
            Map<Integer, Long> endOffsetMap = new HashMap<>();
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
            for (TopicPartition partitionInfo : endOffsets.keySet()) {
                endOffsetMap.put(partitionInfo.partition(), endOffsets.get(partitionInfo));
            }
            Map<Integer, Long> commitOffsetMap = new HashMap<>();

            //查询消费offset
            for (TopicPartition topicAndPartition : topicPartitions) {
                OffsetAndMetadata committed = consumer.committed(topicAndPartition);
                if (committed == null) {
                    commitOffsetMap.put(topicAndPartition.partition(), 0L);
                } else {
                    commitOffsetMap.put(topicAndPartition.partition(), committed.offset());
                }
            }
            //累加lag
            Long lagSum = 0L;
            if (endOffsetMap.size() == commitOffsetMap.size()) {
                for (Integer partition : endOffsetMap.keySet()) {
                    long endOffSet = endOffsetMap.get(partition);
                    long commitOffSet = commitOffsetMap.get(partition);
                    long diffOffset = endOffSet - commitOffSet;
                    lagSum += diffOffset;
                }
            }
            return lagSum.intValue();
        } catch (Exception e) {
            log.error("读取任务出错", e);
            return Integer.MAX_VALUE;
        }
    }

    /**
     * topic下的所有消费者
     */
    private static final Map<String, Consumer<String, String>> CONSUMER_MAP = new ConcurrentHashMap<>();

    private Consumer<String, String> getConsumer(String topic) {
        if (topic == null) {
            return null;
        }
        Consumer<String, String> consumer = CONSUMER_MAP.get(topic);
        if (consumer == null) {
            consumer = this.createConsumer(topic);
            CONSUMER_MAP.put(topic, consumer);
        }
        return consumer;
    }

    private Consumer<String, String> createConsumer(String topic) {
        if (CONSUMER_MAP.containsKey(topic)) {
            return CONSUMER_MAP.get(topic);
        }
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

}
