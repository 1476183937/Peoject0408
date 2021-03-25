package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

public class InterceptorProducer {

    public static void main(String[] args) {

        //设置配置信息
        Properties properties = new Properties();
        //设置连接到的kafka
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //设置key、value的序列化
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //定义拦截器数组
        ArrayList<String> interceptors = new ArrayList<>();
        interceptors.add("com.atguigu.interceptor.CountInterceptor");
        interceptors.add("com.atguigu.interceptor.TimeInterceptor");
        //设置拦截器
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);


        //创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)));

        }

        producer.close();

    }
}
