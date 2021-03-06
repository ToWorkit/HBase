package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class equalsTables {

    public static Configuration conf;

    static {
        conf = new HBaseConfiguration();
    }

    /**
     * 判断表是否存在
     *
     * @param tableName HBaseAdmin(conf) -> 管理表(创建，删除)
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
     *                     HTableDescriptor -> 表描述器，用于创建表
     *                     HColumnDescriptor -> 列描述器，用于构建列族
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

    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        // 建立连接(resource中的配置文件)
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        // 判断表是否存在
        if (isExist(tableName)) {
            // 判断表是否可用(删除表之前必须先将表设置为不可用状态)
            if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
            }
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("删除成功");
        } else {
            System.out.println("表不存在");
        }
    }

    /**
     * 添加数据
     *
     * @param tableName
     * @param rowKey
     * @param cf
     * @param column
     * @param value
     * @throws IOException
     * Table -> 操作表
     * Put -> 封装待存放的数据
     */
    public static void addRow(String tableName, String rowKey, String cf, String column, String value) throws IOException {
        // 建立连接(resource中的配置文件)
        Connection connection = ConnectionFactory.createConnection(conf);
        // 表名 缺少严谨
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 指定rowKey
        Put put = new Put(Bytes.toBytes(rowKey));
        // 列族
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
        // 添加数据
        table.put(put);
        table.close();
    }

    /**
     * 删除一行数据
     *
     * @param tableName
     * @param rowKey
     * @param cf
     * @throws IOException
     * Delete -> 封装待删除的数据
     */
    public static void deleteRow(String tableName, String rowKey, String cf) throws IOException {
        // 建立连接(resource中的配置文件)
        Connection connection = ConnectionFactory.createConnection(conf);
        // 指定表
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
    }

    /**
     * 删除多行(rowKey)数据
     *
     * @param tableName
     * @param rowKeys
     * @throws IOException
     * Delete -> 封装待删除的数据
     */
    public static void deleteMultiRow(String tableName, String... rowKeys) throws IOException {
        // 建立连接(resource中的配置文件)
        Connection connection = ConnectionFactory.createConnection(conf);
        // 指定表
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> list = new ArrayList<>();
        for (String row : rowKeys) {
            Delete delete = new Delete(Bytes.toBytes(row));
            list.add(delete);
        }
        table.delete(list);
        table.close();
    }

    /**
     * 查询数据
     * @param tableName
     * @throws IOException
     * Scan -> 扫描表的配置信息
     * ResultScanner -> 通过配置的扫描器，得到一个扫描表的实例扫描器
     * Result -> 每一个该类型的实例化对象，都对应了一个rowkey中的若干数据
     * Cell -> 用于封装一个rowkey下面所有单元格中的数据(rowkey, cf, cn, value)
     */
    public static void getAllRows(String tableName) throws IOException {
        // 建立连接(resource中的配置文件)
        Connection connection = ConnectionFactory.createConnection(conf);
        // 指定表
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            Cell[] cells = result.rawCells();
/*            for (Cell cell : cells) {
                System.out.println("行键 " + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列 " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值 " + Bytes.toString(CellUtil.cloneValue(cell)));
            }*/
        }
    }

    /**
     * 获取指定的数据
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName, String rowKey) throws IOException {
        // 建立连接(resource中的配置文件)
        Connection connection = ConnectionFactory.createConnection(conf);
        // 指定表
        Table table = connection.getTable(TableName.valueOf(tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        // 列族，列名
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("行键 " + Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("列族 " + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列 " + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值 " + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void main(String[] args) throws IOException {
//        System.out.println(isExist("student"));
//        createTable("test", "info", "info01");
//        deleteTable("test");
//        addRow("student", "101", "info", "name", "pl");
//        addRow("student", "102", "info", "name", "lp");
//        addRow("student", "103", "info", "name", "pl");
//        deleteRow("student", "101", "info");
//        deleteMultiRow("student", "101", "103");
//        getAllRows("student");
        getRow("student", "101");
    }
}
