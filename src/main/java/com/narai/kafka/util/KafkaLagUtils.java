package com.narai.kafka.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;

/**
 * @author: kaze
 * @date: 2019-02-23
 */
@Component
public class KafkaLagUtils {

    private static ConsumerFactory<String, Object> consumerFactory;

    @Resource
    public void setConsumerFactory(ConsumerFactory<String, Object> consumerFactory) {
        KafkaLagUtils.consumerFactory = consumerFactory;
    }

    /**
     * 获取某主题下还剩多少消息
     */
    public Integer getTopicLag(String topic) {
        try {
            Consumer<String, Object> consumer = consumerFactory.createConsumer();
            consumer.subscribe(Collections.singletonList(topic));
            List<TopicPartition> topicPartitions = new ArrayList<>();
            List<PartitionInfo> partitionsFor = consumer.partitionsFor(topic);
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
            return -1;
        }
    }

}
