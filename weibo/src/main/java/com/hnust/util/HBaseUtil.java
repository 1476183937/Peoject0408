package com.hnust.util;

import com.hnust.constants.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 1、创建命名空间
 * 2、判断表是否存在
 * 3、创建三张表
 */
public class HBaseUtil {

    //1、创建命名空间
    public static void createNameSpace(String nameSpace) throws IOException {

        //1、获取COnnection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //2、获取admin对象
        Admin admin = connection.getAdmin();

        //3、创建命名空间描述器
        NamespaceDescriptor build = NamespaceDescriptor.create(nameSpace).build();

        //4、创建命名空间
        admin.createNamespace(build);

        //5、关闭资源
        connection.close();
        admin.close();

    }

    //2、判断表是否存在
    public static boolean isTableExits(String tableName) throws IOException {

        //1、获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //2、获取admin对象
        Admin admin = connection.getAdmin();

        //3、判断表是否存在
        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));

        //4、关闭资源
        admin.close();

        return tableExists;
    }

    //3、创建三张表
    public static void createTables(String tableName, int versions, String... cfs) throws IOException {

        //1、判断是否传入子列族信息
        if (cfs.length <= 0){
            System.out.println("请传入列族信息");
            return;
        }

        //2、判断表是否存在
        if (isTableExits(tableName)){
            System.out.println(tableName + "表已存在！！！");
        }

        //3、获取connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //4、获取admin对象
        Admin admin = connection.getAdmin();

        //5、创建表描述器

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //6、添加列族信息
        for (String cf : cfs) {

            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

            //7、设置版本
            hColumnDescriptor.setMaxVersions(versions);

            //添加列族信息
            tableDescriptor.addFamily(hColumnDescriptor);


        }

        //8、创建表
        admin.createTable(tableDescriptor);

        //9、关闭资源
        admin.close();
        connection.close();

    }



}
