package com.atguigu.producer;

import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyProducer {

    public static void main(String[] args) {


        //设置配置信息
        Properties properties = new Properties();
        //设置连接到的kafka
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //设置key、value的序列化
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        properties.put("acks", "all");
        //重试次数
        properties.put("retries", 1);
        //批次大小
        properties.put("batch.size", 16384);
        //等待时间
        properties.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小
        properties.put("buffer.memory", 33554432);

        //创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

//            producer.send(new ProducerRecord<>("first","hnust"));
            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)));

        }

        producer.close();


    }
}
