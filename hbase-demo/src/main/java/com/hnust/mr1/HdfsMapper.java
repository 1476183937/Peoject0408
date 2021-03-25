package com.hnust.mr1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HdfsMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //读取一行
        String line = value.toString();

        String[] fields = line.split("\t");

        //初始化rowKey对象
        Put put = new Put(Bytes.toBytes(fields[0]));
        //初始化ImmutableBytesWritable对象
        ImmutableBytesWritable bytesWritable = new ImmutableBytesWritable(Bytes.toBytes(fields[0]));
        //设置列族、列、值
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(fields[1]));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("color"),Bytes.toBytes(fields[2]));

        context.write(bytesWritable,put);

    }
}
