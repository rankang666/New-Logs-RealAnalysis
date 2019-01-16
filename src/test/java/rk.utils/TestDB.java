package rk.utils;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import rk.news.utils.DBCPUtils;

import java.sql.SQLException;

/**
 * @Author rk
 * @Date 2018/12/1 14:28
 * @Description:
 **/
public class TestDB {
    public static void main(String[] args) throws SQLException {
        QueryRunner qr = new QueryRunner(DBCPUtils.getDataSource());
        String sql = "select * from b_hour_topics";
        Long count = qr.query(sql, new ScalarHandler<Long>());
        System.out.println(count);

    }


}
