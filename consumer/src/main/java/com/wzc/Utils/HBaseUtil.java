package com.wzc.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;

/*
1.创建命名空间
2.判断表是否存在
3.创建表
4.生成rowkey
5.预分区键的生成
 */
public class HBaseUtil {

    private static Configuration conf;
    static {
        conf= HBaseConfiguration.create();
    }

    //1.创建命名空间
    public static void createNamespace(String nameSpace) throws IOException {

        //获取连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        //获取命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        admin.createNamespace(namespaceDescriptor);

        //关闭资源
        admin.close();
        connection.close();
    }

    //2.判断表是否存在
    public static boolean existTable(String tableName) throws IOException {

        //获取连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        //判断
        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));

        //关闭资源
        admin.close();
        connection.close();

        return tableExists;
    }

    //3.创建表
    public static void createTable(String tableName, Integer regions, String... cfs) throws IOException {

        //获取连接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        //判断表是否存在
        if (existTable(tableName)){
            System.out.println("表："+tableName+"已存在！");
            admin.close();
            connection.close();
            return;
        }

        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //循环添加列族
        for (String cf : cfs) {
            //创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        //添加协处理器
        //hTableDescriptor.addCoprocessor();
        //创建表
        admin.createTable(hTableDescriptor);

        //关闭资源
        admin.close();
        connection.close();
    }

    //4.生成rowkey
    //例如：0x_13712341234_2019-06-01 12:33:55_时间戳_13598769876_duration
    public static String getRowKey(String rowHash, String caller,
                                   String buildTime, Long buildTS, String callee, String flag,String duration){

        return   rowHash+"_"
                +caller+"_"
                +buildTime+"_"
                +buildTS+"_"
                +callee+"_"
                +flag+"_"
                +duration;
    }

    //生成分区号
    public static String getRowHash(Integer regions,String caller,String buildTime){

        DecimalFormat df = new DecimalFormat("00");

        //取手机号中间4位
        String phoneSub = caller.substring(3, 7);
        String yearMonth = buildTime.replace("-", "").substring(0, 6);

        int i = (Integer.valueOf(phoneSub) ^ Integer.valueOf(yearMonth)) % regions;

        return df.format(i);
    }

    //5.预分区键的生成   ----->  00|01|02|03|04|05|
    public static byte[][] getSplitKeys(Integer regions){

        //创建分区键二维数组
        byte[][] splitKeys = new byte[regions][];

        //循环添加分区键
        for (int i = 0; i <regions ; i++) {
            splitKeys[i] = Bytes.toBytes(new DecimalFormat("00").format(i)+"|");
        }
        return splitKeys;
    }
}
