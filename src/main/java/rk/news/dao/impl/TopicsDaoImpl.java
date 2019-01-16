package rk.news.dao.impl;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import redis.clients.jedis.Jedis;
import rk.news.conf.Constants;
import rk.news.dao.ITopicsDao;
import rk.news.utils.DBCPUtils;
import rk.news.utils.JedisUtil;

import java.sql.SQLException;
import java.util.List;

/**
 * @Author rk
 * @Date 2018/12/3 18:38
 * @Description:
 **/
public class TopicsDaoImpl implements ITopicsDao {
    @Override
    public long getAllTopics() {
        Jedis jedis = JedisUtil.getJedis();
        Long totalTopics = jedis.scard(Constants.NEW_TOPICS);
        JedisUtil.close(jedis);
        return totalTopics;
    }

    private QueryRunner qr = new QueryRunner(DBCPUtils.getDataSource());
    @Override
    public List<Object[]> newsTopN() {
        try {
            return qr.query("select topic, times from b_topics_top",
                    new ArrayListHandler());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
