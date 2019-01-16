package rk.utils;

import redis.clients.jedis.Jedis;
import rk.news.conf.Constants;
import rk.news.utils.JedisUtil;

public class TestRedis {
    public static void main(String[] args) {
        Jedis jedis = JedisUtil.getJedis();
        Long totalTopics = jedis.scard(Constants.NEW_TOPICS);
        System.out.println(totalTopics);
        JedisUtil.close(jedis);
    }
}
