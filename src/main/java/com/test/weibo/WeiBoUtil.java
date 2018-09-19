package com.test.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

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
        String rowKey = uid + "_" + ts;

        Put put = new Put(Bytes.toBytes(rowKey));
        // 手动传入时间戳防止卡顿或者其他因素影响导致时间不一致
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), ts, Bytes.toBytes(value));
        // 添加数据
        table.put(put);


        /**
         * 发微博后所有的数据都需要更新
         */
        Table inboxTable = connection.getTable(TableName.valueOf(Contents.INBOX_TABLE));
        Table relationTable = connection.getTable(TableName.valueOf(Contents.RELATION_TABLE));

        Get get = new Get(Bytes.toBytes(uid));
        Result result = relationTable.get(get);

        ArrayList<Put> puts = new ArrayList<>();

        for (Cell cell : result.rawCells()) {
            if ("fans".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                byte[] inboxRowKey = CellUtil.cloneQualifier(cell);

                Put inboxPut = new Put(inboxRowKey);
                // 收件箱表存放的是内容表的rowKey
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), ts, Bytes.toBytes(rowKey));

                puts.add(inboxPut);
            }
        }
        // 多条数据(所有的粉丝)一起添加
        inboxTable.put(puts);

        table.close();
        inboxTable.close();
        relationTable.close();
        connection.close();
    }

    /**
     * 添加关注用户 (多个)
     * 1. 在用户关系表中，给当前用户添加attends
     * 2. 在用户关系表中，给被关注用户添加fans
     * 3. 在收件箱表中，给当前用户添加关注用户最近所发的微博rowKey
     */
    public static void addAttends(String uid, String... attends) throws IOException {
        // 1. 在用户关系表中，给当前用户添加attends
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(Contents.RELATION_TABLE));
        // 给当前用户添加
        Put attendPut = new Put(Bytes.toBytes(uid));

        // 存放被关注用户的添加对象
        ArrayList<Put> puts = new ArrayList<>();
        puts.add(attendPut);

        for (String attend : attends) {
            attendPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend), Bytes.toBytes(""));
            // 2. 在用户关系表中，给被关注用户添加fans
            Put put = new Put(Bytes.toBytes(attend));
            put.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(""));
            puts.add(put);
        }
        table.put(puts);


        // 3. 在收件箱表中，给当前用户添加关注用户最近所发的微博rowKey
        // 重新获取表(收件箱表)
        Table inboxTable1 = connection.getTable(TableName.valueOf(Contents.INBOX_TABLE));
        // 微博内容表
        Table connectionTable = connection.getTable(TableName.valueOf(Contents.CONTENT_TABLE));
        Put inboxPut = new Put(Bytes.toBytes(uid));

        // 循环添加数据
        for (String attend : attends) {
            // 通过startRow和stopRow构建扫描器 第一种方式
//            Scan scan = new Scan(Bytes.toBytes(attend), Bytes.toBytes(attend + "|"));

            // 通过过滤器构建扫描器
            Scan scan = new Scan();
            RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_"));
            scan.setFilter(rowFilter);
            // 获取所有符合扫描规则的数据
            ResultScanner scanner = connectionTable.getScanner(scan);

            // 循环遍历取出每条数据的rowKey添加到inboxPut中
            for (Result result : scanner) {
                byte[] row = result.getRow();
                inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attend), row);
                // 往收件箱表中给操作者添加数据
                inboxTable1.put(inboxPut);
            }
        }



        table.close();
        inboxTable1.close();
        connectionTable.close();
        connection.close();
    }


    /**
     * 取关
     * 1. 在用户关系表中，删除当前用户的attends
     * 2. 在用户关系表中，删除被取关用户的fans(操作者)
     * 3. 在收件箱表中删除取关用户的所有数据
     */
    public static void deleteRelation(String uid, String... deletes) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table relationTable = connection.getTable(TableName.valueOf(Contents.RELATION_TABLE));

        ArrayList<Delete> deleteArrayList = new ArrayList<>();

        // 1. 在用户关系表中，删除当前用户的attends
        Delete userDelete = new Delete(Bytes.toBytes(uid));
        for (String delete : deletes) {
            // 操作者删除关注
            userDelete.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(delete));

            // 2. 在用户关系表中，删除被取关用户的fans(操作者)
            // 被操作者删除粉丝
            Delete fanDelete = new Delete(Bytes.toBytes(delete));
            fanDelete.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid));

            deleteArrayList.add(fanDelete);
        }
        deleteArrayList.add(userDelete);

        relationTable.delete(deleteArrayList);

        // 3.在收件箱表中删除取关用户的所有数据
        Table inboxTable = connection.getTable(TableName.valueOf(Contents.INBOX_TABLE));
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));

        for (String delete : deletes) {
            inboxDelete.addColumns(Bytes.toBytes("info"), Bytes.toBytes(delete));
        }
        inboxTable.delete(inboxDelete);

        relationTable.close();
        inboxTable.close();
        connection.close();
    }

    /**
     * 获取关注人的微博内容
     */
    public static void getWeibo(String uid) throws IOException {
        // 内容表和收件箱表
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table inboxTable = connection.getTable(TableName.valueOf(Contents.INBOX_TABLE));
        Table contentTable = connection.getTable(TableName.valueOf(Contents.CONTENT_TABLE));

        // 设置版本(获取收件箱表的三条数据)
        Get get = new Get(Bytes.toBytes(uid));
        get.setMaxVersions(3);
        Result result = inboxTable.get(get);

        for (Cell cell : result.rawCells()) {
            // 通过收件箱表获取到的数据查询内容表中对应的内容
            byte[] contentRowKey = CellUtil.cloneValue(cell);
            Get contentGet = new Get(contentRowKey);
            Result contentResult = contentTable.get(contentGet);
            for (Cell cell1 : contentResult.rawCells()) {
                String uid_ts = Bytes.toString(CellUtil.cloneRow(cell1));
                String id = uid_ts.split("_")[0];
                String ts = uid_ts.split("_")[1];

                String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(Long.parseLong(ts)));
                System.out.println("用户: " + id + ", 时间: " + date + " 发布了 " + Bytes.toString(CellUtil.cloneValue(cell1)));
            }
        }

        inboxTable.close();
        contentTable.close();
        connection.close();
    }
}
