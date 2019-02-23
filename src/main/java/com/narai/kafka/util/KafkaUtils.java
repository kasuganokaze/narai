package com.narai.kafka.util;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.*;

/**
 * @author: kaze
 * @date: 2019-02-20
 */
@Component
public class KafkaUtils {

    private static ConsumerFactory<String, Object> consumerFactory;

    @Resource
    public void setConsumerFactory(ConsumerFactory<String, Object> consumerFactory) {
        KafkaUtils.consumerFactory = consumerFactory;
    }

    /**
     * 创建主题
     */
    public static void createKafkaTopic(String zkUrl, String topic, int partitions, int replicationFactor) {
        ZkUtils zkUtils = ZkUtils.apply(zkUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        AdminUtils.createTopic(zkUtils, topic, partitions, replicationFactor, new Properties(), AdminUtils.createTopic$default$6());
    }

    /**
     * 删除主题
     */
    public static void deleteKafkaTopic(String zkUrl, String topic) {
        ZkUtils zkUtils = ZkUtils.apply(zkUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        AdminUtils.deleteTopic(zkUtils, topic);
        zkUtils.close();
    }

    /**
     * 发送消息
     */
    public static void produce(String kafkaUrl, String topic, Object message) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 10);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        Producer<String, Object> producer = new KafkaProducer<>(properties);
        producer.send(new ProducerRecord<>(topic, message));
    }

    /**
     * 消费消息
     */
    public static Object consume(String kafkaUrl, String groupId, String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<String, Object> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<String, Object> records = consumer.poll(1000);
        for (ConsumerRecord<String, Object> record : records) {
            return record.value();
        }
        return null;
    }

    /**
     * 获取某主题下还剩多少消息
     */
    public Integer getLagByTopic(String topic) {
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
