package rk.news.utils;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author rk
 * @Date 2018/12/3 18:40
 * @Description:
 **/
public class DateUtil {
    static DateFormat df_yyyy_MM_dd = new SimpleDateFormat("yyyy-MM-dd");

    public static String getTodayDate(){
        return df_yyyy_MM_dd.format(new Date());
    }
}
