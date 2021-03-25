package com.hnust.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HbaseRunner implements Tool {

    private Configuration configuration;

    @Override
    public int run(String[] strings) throws Exception {

        //获取job对象
        Job job = Job.getInstance(configuration);

        job.setJarByClass(HbaseRunner.class);

        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

        //设置mapper
        TableMapReduceUtil.initTableMapperJob("fruit", //设置map输入时扫描的表
                scan, //设置scan
                HbaseMapper.class, //设置mapper类
                ImmutableBytesWritable.class, //设置map阶段输出时key的类型
                Put.class, //设置map阶段输出时value的类型
                job);//设置job

        //设置Reducer
        TableMapReduceUtil.initTableReducerJob("fruit_mr", HbaseReducer.class, job);

        job.setNumReduceTasks(1);
        boolean result = job.waitForCompletion(true);

        if (!result){
            System.out.println("Job something erroe");
        }

        return result ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;

    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();

        int run = ToolRunner.run(conf, new HbaseRunner(), args);

        System.exit(run);

    }





}
