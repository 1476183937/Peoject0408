package com.hnust.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HbaseRunner implements Tool {

    private Configuration configuration;

    @Override
    public int run(String[] args) throws Exception {

        //1、获取Job对象
        Job job = Job.getInstance(configuration);

        //2、设置Jarlei
        job.setJarByClass(HbaseRunner.class);

        //3、设置mapper,reducer
        job.setMapperClass(HdfsMapper.class);
        job.setReducerClass(HbaseReducer.class);

        //4、设置mapper输出类型
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //5、设置reduce输出类型,要使用TableMapReduceUtil
        TableMapReduceUtil.initTableReducerJob("fruit_mr", HbaseReducer.class, job);

        //6、设置输入路径
        Path inPath = new Path("hdfs://hadoop102:9000/input_fruit/fruit.tsv");
        FileInputFormat.setInputPaths(job, inPath);

        boolean result = job.waitForCompletion(true);

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

        Configuration configuration = HBaseConfiguration.create();
        int run = ToolRunner.run(configuration, new HbaseRunner(), args);
        System.exit(run);
    }
}
