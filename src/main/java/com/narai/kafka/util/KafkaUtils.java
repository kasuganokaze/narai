package com.narai.kafka.util;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;

/**
 * @author: kaze
 * @date: 2019-02-20
 */
public class KafkaUtils {

    /**
     * 删除主题
     */
    public static void deleteKafkaTopic(String zkUrl, String topicName) {
        ZkUtils zkUtils = ZkUtils.apply(zkUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        AdminUtils.deleteTopic(zkUtils, topicName);
        zkUtils.close();
    }

}
