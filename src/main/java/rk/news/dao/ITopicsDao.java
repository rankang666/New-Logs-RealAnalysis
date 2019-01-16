package rk.news.dao;

import java.util.List;

/**
 * @Author rk
 * @Date 2018/12/3 18:36
 * @Description:
 **/
public interface ITopicsDao {
    /**
     * 查询目前所有的topic个数
     * @return
     */
    long getAllTopics();

    List<Object[]> newsTopN();

}
