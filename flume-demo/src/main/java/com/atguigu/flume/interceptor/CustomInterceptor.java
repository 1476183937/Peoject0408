package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CustomInterceptor implements Interceptor {

    private List<Event> eventList;

    @Override
    public void initialize() {

        eventList = new ArrayList<>();

    }

    @Override
    public Event intercept(Event event) {

        //获取头部
        Map<String, String> headers = event.getHeaders();

        //获取主体部分
        String body = new String(event.getBody());

        //根据body中是否含有hello来决定添加怎样的头信息
        if (body.contains("hello")){
            //内容包含hello
            headers.put("type","atguigu");
        }else {

            headers.put("type","bigdata");
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        for (Event event : list) {
            eventList.add(intercept(event));
        }
        return eventList;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new CustomInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }


}
