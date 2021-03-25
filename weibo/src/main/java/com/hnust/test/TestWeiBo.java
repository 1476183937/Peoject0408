package com.hnust.test;

import com.hnust.constants.Constants;
import com.hnust.dao.HBaseDao;
import com.hnust.util.HBaseUtil;

import java.io.IOException;

public class TestWeiBo {

    public static void init() throws IOException {

        //1、创建命名空间
        HBaseUtil.createNameSpace(Constants.NAMESPACE);

        //2、创建内容表
        HBaseUtil.createTables(Constants.CONTENT_TABLE,Constants.CONTENT_VERSION,Constants.CONTENT_CF);

        //3、创建关系表
        HBaseUtil.createTables(Constants.RELATION_TABLE,Constants.RELATION_VERSION,Constants.RELATION_CF1,Constants.RELATION_CF2);

        //4、创建收件箱表
        HBaseUtil.createTables(Constants.INBOX_TABLE,Constants.INBOX_VERSION,Constants.INBOX_CF);

    }

    public static void main(String[] args) throws IOException, InterruptedException {

        //1、初始化
//        init();

        //2、1001发布微博
        HBaseDao.publishWeiBo("1001","快点下课把");

        //3、1002关注1001和1003
        HBaseDao.addAttends("1002","1001","1003");

        //4、获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("**************111******************");

        //5、1003发布3条微博，同时1002发布2条微博
        HBaseDao.publishWeiBo("1003","谁说的赶紧下课！！！");
        Thread.sleep(100);
        HBaseDao.publishWeiBo("1001","我没有说话");
        Thread.sleep(100);
        HBaseDao.publishWeiBo("1003","那谁说的");
        Thread.sleep(100);
        HBaseDao.publishWeiBo("1001","反正不是我");
        Thread.sleep(100);
        HBaseDao.publishWeiBo("1003","随便你们吧");

        //6、获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("**************222******************");

        //7、1002取关1003
        HBaseDao.deleteAttends("1002","1003");

        //8、获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("**************333******************");

        //9、1002再次关注1003
        HBaseDao.addAttends("1002","1003");

        //10、获取1002初始化页面
        HBaseDao.getInit("1002");

        //11、获取1001的全部微博详情
        HBaseDao.getWeiBo("1001");

    }
}
