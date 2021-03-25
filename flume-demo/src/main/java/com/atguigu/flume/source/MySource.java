package com.atguigu.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    private String prefix;
    private String subfix;

    @Override
    public void configure(Context context) {

        prefix = context.getString("prefix");
        subfix = context.getString("subfix","atguigu");

    }

    /**
     * 1.接收数据
     * 2.封装事件
     * 3.将事件传递给channel
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {

        Status status = null;

        //接收数据，自己造数据
        try {
            for (int i = 0; i < 5; i++) {

                //构建事件对象
                SimpleEvent event = new SimpleEvent();

                //给事件设置值
                event.setBody((prefix +  "--" + i +"--" + subfix).getBytes());

                //将事件传给channel
                getChannelProcessor().processEvent(event);

            }

            status = Status.READY;

        } catch (Exception e) {
            status = Status.BACKOFF;
            e.printStackTrace();
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


}
