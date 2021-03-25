package com.atguigu.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class MySink extends AbstractSink implements Configurable {

    private String prefix;
    private String subfix;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);

    @Override
    public void configure(Context context) {

        prefix = context.getString("prefix");
        subfix = context.getString("subfix","hello");

    }


    @Override
    public Status process() throws EventDeliveryException {

        Status status = null;

        Channel channel = getChannel();
        //获取事务
        Transaction txn = channel.getTransaction();
        //开始事务
        txn.begin();

        //声明事件
        Event event;

        //不断从channel中获取事件，知道不为空
        while (true){

            event = channel.take();
            if (event != null){
                break;
            }
        }


        try {

            //模拟处理过程
            LOG.info(prefix + new String(event.getBody()) + subfix);

            //提交事务
            txn.commit();
            status = Status.READY;
        } catch (Exception e) {
            //回滚事务
            txn.rollback();
            status = Status.BACKOFF;
            e.printStackTrace();
        }finally {
            //关闭事务
            txn.close();
        }


        return status;
    }


}
