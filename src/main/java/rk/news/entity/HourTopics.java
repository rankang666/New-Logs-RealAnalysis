package rk.news.entity;

/**
 * @Author rk
 * @Date 2018/12/3 17:04
 * @Description:
 *      create table b_hour_top(
 *          hour varchar(10),
 *          times int
 *        );
 **/
public class HourTopics {
    private String hour;
    private int times;

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public int getTimes() {
        return times;
    }

    public void setTimes(int times) {
        this.times = times;
    }
}
