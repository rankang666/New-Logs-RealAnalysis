package rk.news.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import rk.news.conf.RedisConfig;

import java.io.IOException;
import java.util.Properties;

/**
 * @Author rk
 * @Date 2018/12/3 19:02
 * @Description:
 **/
public class JedisUtil {
    private static JedisPool jedisPool;
    private JedisUtil(){}
    private static Properties prop;
    static {
        prop = new Properties();
        try {
            prop.load(JedisUtil.class.getClassLoader().getResourceAsStream("redis.conf"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Jedis getJedis(){
        if(jedisPool == null){
            synchronized (JedisUtil.class){
                JedisPoolConfig poolConfig = new JedisPoolConfig();
                poolConfig.setMaxIdle(Integer.valueOf(prop.getProperty(RedisConfig.MAX_IDLE)));
                poolConfig.setMaxTotal(Integer.valueOf(prop.getProperty(RedisConfig.MAX_TOTAL)));
                poolConfig.setMaxWaitMillis(Integer.valueOf(prop.getProperty(RedisConfig.MAX_WAIT_MILLIS)));
                jedisPool = new JedisPool(poolConfig,
                        prop.getProperty(RedisConfig.HOST),
                        Integer.valueOf(prop.getProperty(RedisConfig.PORT)),
                        Integer.valueOf(prop.getProperty(RedisConfig.TIME_OUT)));
            }
        }
        return jedisPool.getResource();
    }
    public static void close(Jedis jedis){
        if (jedis != null){
            jedis.close();
        }
    }
}
