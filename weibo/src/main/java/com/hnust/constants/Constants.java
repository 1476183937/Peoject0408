package com.hnust.constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class Constants {

    //HBase的配置信息
    public static final Configuration CONFIGURATION = HBaseConfiguration.create();

    //命名空间
    public static final String NAMESPACE = "weibo";

    //微博内容表
    public static final String CONTENT_TABLE = "weibo:content";
    //列族
    public static final String CONTENT_CF = "info";
    public static final int CONTENT_VERSION = 1;

    //微博关系表
    public static final String RELATION_TABLE = "weibo:relation";
    //列族
    public static final String RELATION_CF1 = "attends"; //关注的人
    public static final String RELATION_CF2 = "fans";    //粉丝
    public static final int RELATION_VERSION = 1;        //版本

    //微博收件箱表
    public static final String INBOX_TABLE = "weibo:inbox";
    //列族
    public static final String INBOX_CF = "info";    //列族
    public static final int INBOX_VERSION = 2;        //版本





}
