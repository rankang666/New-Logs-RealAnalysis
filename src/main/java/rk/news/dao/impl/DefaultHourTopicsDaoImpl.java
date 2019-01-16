package rk.news.dao.impl;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import rk.news.dao.IHourTopicsDao;
import rk.news.entity.HourTopics;
import rk.news.utils.DBCPUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author rk
 * @Date 2018/12/3 17:08
 * @Description:
 **/
public class DefaultHourTopicsDaoImpl implements IHourTopicsDao {
    private QueryRunner qr = new QueryRunner(DBCPUtils.getDataSource());

    String insertSQL = "INSERT INTO b_hour_topics (hour, times) values(?,?)";
    String updateSQL = "UPDATE b_hour_topics SET times = ? WHERE hour = ? ";
    String selectSQL = "SELECT times FROM b_hour_topics WHERE hour = ?";
    @Override
    public void insert(HourTopics ht) {
        try {
            //首先查看当前小时对应的数据是否存在，如果不存在，则插入，如果存在，覆盖
            Integer times = qr.query(selectSQL, new ScalarHandler<>(), ht.getHour());
            if (times == null ){//数据库中没有
                qr.update(insertSQL, ht.getHour(), ht.getTimes());
            }else{
                qr.update(updateSQL, ht.getTimes(), ht.getHour());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void insertBatch(List<HourTopics> hts) {
        //进行插入的数据
        List<HourTopics> insertList = new ArrayList<>();
        //更新的数据
        List<HourTopics> updateList = new ArrayList<>();

        try {
            //一条一条的判断当前数据库中是否已经存在对应的数据
            for(HourTopics ht : hts){
                Integer times = qr.query(selectSQL, new ScalarHandler<>(), ht.getHour());
                if (times == null ){//数据库中没有
                    insertList.add(ht);
                }else{
                    updateList.add(ht);
                }
            }
            //执行插入
            if(!insertList.isEmpty()){
                Object[][] insertParams = new Object[insertList.size()][];

                for (int i = 0; i < insertList.size(); i++){
                    HourTopics ht = insertList.get(i);
                    Object[] obj = {ht.getHour(),ht.getTimes()};
                    insertParams[i] = obj;
                }
                qr.batch(insertSQL,insertParams);
            }


            //执行更新
            if(!updateList.isEmpty()){
                Object[][] updateParams = new Object[updateList.size()][];

                for (int i = 0; i < updateList.size(); i++){
                    HourTopics ht = updateList.get(i);
                    Object[] obj = {ht.getTimes(),ht.getHour()};
                    updateParams[i] = obj;
                }
                qr.batch(updateSQL,updateParams);
            }



        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    @Override
    public List<Object[]> getHourTopics() {
        String sql = "select hour, times from b_hour_topics";
        try {
            return qr.query(sql,new ArrayListHandler());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        DefaultHourTopicsDaoImpl dao = new DefaultHourTopicsDaoImpl();
        List<Object[]> list = dao.getHourTopics();
        for(Object[] objs : list){
            System.out.println(Arrays.toString(objs));
        }
    }
}
