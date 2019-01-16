package rk.news.web;

import org.json.JSONException;
import org.json.JSONObject;
import rk.news.dao.IHourTopicsDao;
import rk.news.dao.ITopicsDao;
import rk.news.dao.impl.DefaultHourTopicsDaoImpl;
import rk.news.dao.impl.TopicsDaoImpl;
import rk.news.utils.DateUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author rk
 * @Date 2018/12/3 19:39
 * @Description:
 **/
public class ShowServlet extends HttpServlet {
    private ITopicsDao topicsDao = new TopicsDaoImpl();
    private IHourTopicsDao hourTopicsDao = new DefaultHourTopicsDaoImpl();

    @Override
    protected void doGet(HttpServletRequest requset, HttpServletResponse response)
            throws ServletException, IOException {
        String method = requset.getParameter("method");
        response.setContentType("application/json;charset=UTF-8");
        if(method.equalsIgnoreCase("getAllTopics")){
            getAllTopics(requset,response);
        }else if (method.equalsIgnoreCase("newsTopN")){
            newsTopN(requset,response);
        }else if (method.equalsIgnoreCase("hourTopics")){
            hourTopics(requset,response);
        }

    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request,response);
    }

    private void getAllTopics(HttpServletRequest requset, HttpServletResponse response) {
        long totalTopics = topicsDao.getAllTopics();
        try {
            PrintWriter pw = response.getWriter();
            JSONObject jsonObj = new JSONObject();
            jsonObj.put("totalTopics",totalTopics);
//            jsonObj.put("totalTopics",1000);
            pw.write(jsonObj.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    private void newsTopN(HttpServletRequest requset, HttpServletResponse response) {
        List<Object[]> list = topicsDao.newsTopN();
        List<Object> topics = new ArrayList<>();
        List<Object> topicNums = new ArrayList<>();
        for (Object[] objs : list){
            topics.add(objs[0]);
            topicNums.add(objs[1]);
        }
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("date", DateUtil.getTodayDate());
            jsonObject.put("topics", topics);
            jsonObject.put("topicsTimes", topicNums);
            PrintWriter pw = response.getWriter();
            pw.write(jsonObject.toString());
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    private void hourTopics(HttpServletRequest requset, HttpServletResponse response) {
        List<Object[]> list = hourTopicsDao.getHourTopics();
        List<Object> hours = new ArrayList<>();
        List<Object> topicNums = new ArrayList<>();
        for (Object[] objs : list){
            hours.add(objs[0]);
            topicNums.add(objs[1]);
        }
        JSONObject jsonObj = new JSONObject();
        try {
            jsonObj.put("hourList",hours);
            jsonObj.put("topicNumList",topicNums);
            PrintWriter pw = response.getWriter();
            pw.write(jsonObj.toString());
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
