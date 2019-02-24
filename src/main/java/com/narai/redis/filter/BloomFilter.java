package com.narai.redis.filter;

import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.MurmurHash;

import javax.annotation.Resource;

/**
 * @author: kaze
 * @date: 2019-02-23
 */
@Component
public class BloomFilter {

    @Resource
    private JedisPool jedisPool;

    /**
     * 约256M
     */
    private static final Integer BIT_SET_SIZE = Integer.MAX_VALUE;
    /**
     * size越大越耗时间，越小误差越大
     */
    private static final Integer[] SEEDS = new Integer[8];

    static {
        for (int i = 0; i < SEEDS.length; i++) {
            SEEDS[i] = i * SEEDS.length + 1;
        }
    }

    public void setBit(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            for (Integer seed : SEEDS) {
                int hash = MurmurHash.hash(value.getBytes(), seed);
                jedis.setbit(key.getBytes(), Math.abs(hash % BIT_SET_SIZE), true);
            }
        }
    }

    public boolean exists(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            for (Integer seed : SEEDS) {
                int hash = MurmurHash.hash(value.getBytes(), seed);
                if (!jedis.getbit(key.getBytes(), Math.abs(hash % BIT_SET_SIZE))) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 设置过期时间
     */
    public void expire(String key, int expire) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.expire(key.getBytes(), expire);
        }
    }

    /**
     * 删除过期时间
     */
    public void persist(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.persist(key.getBytes());
        }
    }

    /**
     * 删除key
     */
    public void del(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key.getBytes());
        }
    }

    /**
     * 统计bit数
     */
    public Long bitcount(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.bitcount(key.getBytes());
        }
    }

}
