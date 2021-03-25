package com.hnust.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import sun.net.www.ParseUtil;

import java.io.IOException;

public class HbaseReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {

        //将读出来的数据写出去
        for (Put value : values) {

            context.write(NullWritable.get(),value);
        }
    }
}
