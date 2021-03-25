package com.atguigu.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceptor implements ProducerInterceptor<String,String> {
    @Override
    public ProducerRecord onSend(ProducerRecord record) {

        //该拦截器在消息前面添加时间戳
        return new ProducerRecord(record.topic(),record.partition(),record.timestamp(),record.key(),System.currentTimeMillis() + "--" +record.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
