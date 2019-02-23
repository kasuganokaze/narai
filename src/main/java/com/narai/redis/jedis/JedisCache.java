package com.narai.redis.jedis;

import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.annotation.Resource;
import java.io.Serializable;
import java.util.*;

import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;

/**
 * @author: kaze
 * @date: 2019-02-23
 */
@Component
public class JedisCache {

    @Resource
    private static JedisPool jedisPool;

    /**
     * string 类型开始
     */
    public void set(String key, Object value) {
        try (Jedis jedis = jedisPool.getResource()) {
            byte[] rawKey = key.getBytes();
            byte[] rawValue = serialize((Serializable) value);
            jedis.set(rawKey, rawValue);
        }
    }

    public <V> V get(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            byte[] rawValue = jedis.get(key.getBytes());
            if (rawValue == null) {
                return null;
            }
            return deserialize(rawValue);
        }
    }
    // --------------------------------------------------

    /**
     * hash类型开始
     */
    public void hset(String key, String field, Object value) {
        try (Jedis jedis = jedisPool.getResource()) {
            byte[] rawKey = key.getBytes();
            byte[] fieldKey = field.getBytes();
            byte[] rawValue = serialize((Serializable) value);
            jedis.hset(rawKey, fieldKey, rawValue);
        }
    }

    public <V> V hget(String key, String field) {
        try (Jedis jedis = jedisPool.getResource()) {
            byte[] rawValue = jedis.hget(key.getBytes(), field.getBytes());
            if (rawValue == null) {
                return null;
            }
            return deserialize(rawValue);
        }
    }

    public Long hdel(String key, String field) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.hdel(key, field);
        }
    }

    public Map<String, Object> hgetAll(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<byte[], byte[]> rawMap = jedis.hgetAll(key.getBytes());
            if (CollectionUtils.isEmpty(rawMap)) {
                return new HashMap<>();
            }
            Map<String, Object> newMap = new HashMap<>();
            rawMap.forEach((k, v) -> {
                newMap.put(new String(k), deserialize(v));
            });
            return newMap;
        }
    }
    // --------------------------------------------------


    /**
     * 分布式锁开始
     */
    public Boolean setLock(String key, String value, Long expireTime) {
        try (Jedis jedis = jedisPool.getResource()) {
            String result = jedis.set(key, value, "NX", "PX", expireTime);
            return Objects.equals("OK", result);
        }
    }

    public Boolean delLock(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
            Object result = jedis.eval(script, Collections.singletonList(key), Collections.singletonList(value));
            return Objects.equals(1L, result);
        }
    }
    // --------------------------------------------------

    /**
     * 删除key
     */
    public Long del(String key) {
        try (Jedis jedis = jedisPool.getResource();) {
            return jedis.del(key);
        }
    }

    /**
     * 加上过期时间
     */
    public Long expire(String key, int second) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.expire(key.getBytes(), second);
        }
    }

    /**
     * 判断是否存在
     */
    public Boolean exists(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(key.getBytes());
        }
    }

    /**
     * 删除过期时间
     */
    public void persist(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.persist(key);
        }
    }

    /**
     * 获取某前缀下的所有key
     */
    public Set<String> keys(String keyPrefix) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.keys(keyPrefix + "*");
        }
    }

}
