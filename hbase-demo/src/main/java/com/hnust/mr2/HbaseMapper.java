package com.hnust.mr2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        //从Hbase中读取数据，只提取出name一列，并将提取出的数据写回Hbase的另一张fruit2表
        //
        Put put = new Put(key.get());

        //遍历
        for (Cell cell : value.rawCells()) {

            //过滤出name列
            if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                put.add(cell);
            }

        }

        //写出
        context.write(key,put);
    }
}
