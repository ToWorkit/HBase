package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class equalsTables {

    public static Configuration conf;

    static {
        conf = new HBaseConfiguration();
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     */
    public static boolean isExist(String tableName) throws IOException {
        // 老api
//        HBaseAdmin admin = new HBaseAdmin(conf);
//        return admin.tableExists(Bytes.toBytes(tableName));

        // 新api
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 创建表
     *
     * @param tableName    表名
     * @param columnFamily 列族(可传多个)
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        // 建立连接(resource中的配置文件)
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        // 检测表是否存在
        if (isExist("test")) {
            System.out.println("表已存在");
        } else {
            // 先创建表的描述信息
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : columnFamily) {
                hTableDescriptor.addFamily(new HColumnDescriptor(cf));
            }

            // 创建表
            admin.createTable(hTableDescriptor);
            System.out.println("创建成功");
        }
    }

    public static void main(String[] args) throws IOException {
//        System.out.println(isExist("student"));
        createTable("test", "info", "info01");
    }
}
