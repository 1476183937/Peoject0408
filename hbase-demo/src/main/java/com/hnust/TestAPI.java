package com.hnust;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.join.ComposableInputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * DDL：
 * 1、判断表是否存在
 * 2、创建表
 * 3、创建命名空间
 * 4、删除表
 * <p>
 * DML：
 * 5、插入数据
 * 6、查数据（get）
 * 7、查数据（scan）
 * 8、删除数据
 */
public class TestAPI {

    private static Configuration conf;
    private static HBaseAdmin admin;
    private static Connection connection;

    static {
        try {

            conf = HBaseConfiguration.create();
            //连接到集群
            conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            //获取admin对象
            connection = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) connection.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //1、判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {

        boolean exists = admin.tableExists(Bytes.toBytes(tableName));

        return exists;
    }

    public static void createTable(String tableName, String... cfs) throws IOException {

        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已经存在！！！");
        } else {
            //创建表描述
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : cfs) {
                //添加列族
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                descriptor.addFamily(columnDescriptor);
            }

            //创建表
            admin.createTable(descriptor);
            System.out.println(tableName + "创建成功！！！");
        }

    }

    //3、创建命名空间
    public static void createNameSpace(String nameSpace) throws IOException {

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        admin.createNamespace(namespaceDescriptor);
        System.out.println(nameSpace + "创建成功！！！");

    }

    //4、删除表
    public static void deleteTable(String tableName) throws IOException {

        if (!isTableExist(tableName)) {
            System.out.println(tableName + "不存在");
        } else {
            //必须先让表不可以在删除表，否则报错
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println(tableName + "删除成功！！！");

        }
    }

    //5、插入数据
    public static void insertData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {

        //获取表
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        //添加列
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));

        //插入数据
        table.put(put);

        System.out.println("数据插入完成！！！");

    }

    //6、查数据（get）
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));

        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("CF: " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("CN: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("value: " + Bytes.toString(CellUtil.cloneValue(cell)));
        }


    }


    // * 7、查数据（scan）
    public static void scanData(String tableName) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));
//        Scan scan = new Scan(); //扫描全部数据
        //扫描rowKey在1001-1003之间数据，左闭右开
        Scan scan = new Scan(Bytes.toBytes("1001"),Bytes.toBytes("1003"));

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + "--" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "--" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    //  * 8、删除数据
    public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {

        //创建table对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建Delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //addColumn在flush之前删除数据，会删除时间戳最大的数据，再次查询时会返回剩余版本中时间戳虽大的数据,在flush之后则会
        //删除所有版本的数据，再次查询就不会有数据了，如果方法参数里添加了时间戳，则删除指定戳的对应数据
//        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        //addColumns会删除所有版本的数据
        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
        //删除数据
        table.delete(delete);
        System.out.println("删除成功！！！");

    }


    public static void main(String[] args) throws IOException {

        //1、判断表是否存在
//        System.out.println(isTableExist("stu1"));

        //2、创建表
//        createTable("stu2","info1","info2");

        //3、创建命名空间
//        createNameSpace("myhbase");

        //4、删除表
//        deleteTable("stu2");
        //5、插入数据
//        insertData("stu2","1001","info1","age","13");
        //6、get方式查询数据
//        getData("stu2", "1001", "info1", "name");

        //7、scan方式查询数据
//        scanData("stu2");

        //8、删除数据
        deleteData("stu2", "1001", "info1", "name");
    }




}
