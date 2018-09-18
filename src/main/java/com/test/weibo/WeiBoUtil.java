package com.test.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 为什么重复代码没有抽取
 *  因为业务线基本上只会使用一次或者几次，并不会大量的重复使用，所以用完就关闭
 */
public class WeiBoUtil {
    // 获取 HBase 配置信息
    static Configuration configuration = HBaseConfiguration.create();
    /**
     * 创建命名空间
     * @param ns
     * @throws IOException
     */
    public static void createNamespace(String ns) throws IOException {

        // 建立连接并获取HBase管理员对象admin
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        // 构建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        // 创建nameSpace
        admin.createNamespace(namespaceDescriptor);

        admin.close();
        connection.close();
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param versions 版本号
     * @param cfs 列族
     */
    public static void createTable(String tableName, int versions, String... cfs) throws IOException {

        // 建立连接并获取HBase管理员对象admin
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        // 表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        // 列描述器
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            // 设置版本
            hColumnDescriptor.setMaxVersions(versions);
            // 将每个列描述器都放入表描述器中
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        // 创建表
        admin.createTable(hTableDescriptor);

        admin.close();
        connection.close();
    }

    /**
     * 添加数据
     * @param tableName
     * @param uid rowKey
     * @param cf 列族
     * @param cn 列
     * @param value 值
     */
    public static void putData(String tableName, String uid, String cf, String cn, String value) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));

        // 当前时间戳
        long ts = System.currentTimeMillis();
        // rowKey
        String rowKey = uid + ts;

        Put put = new Put(Bytes.toBytes(rowKey));
        // 手动传入时间戳防止卡顿或者其他因素影响导致时间不一致
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), ts, Bytes.toBytes(value));
    }
}
