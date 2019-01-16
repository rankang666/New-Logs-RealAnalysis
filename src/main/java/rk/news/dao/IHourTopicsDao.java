package rk.news.dao;

import rk.news.entity.HourTopics;

import java.util.List;

/**
 * @Author rk
 * @Date 2018/12/3 17:06
 * @Description:
 **/
public interface IHourTopicsDao {
    void insert(HourTopics ht);

    void insertBatch(List<HourTopics> hts);

    List<Object[]> getHourTopics();

}
