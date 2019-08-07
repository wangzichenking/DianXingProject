package com.wzc.consumer;


import com.wzc.Utils.HBaseUtil;
import com.wzc.Utils.PropertityUtil;
import com.wzc.constant.Constant;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/*
1.初始化命名空间
2.创建表
3.批量创建
 */
public class HBaseDao {

    //配置信息
    private Properties properties;
    //命名空间
    private String nameSpace;
    //表名
    private String tableName;
    //分区
    private Integer regions;
    //列族
    private String cf;
    //作为初始化
    private SimpleDateFormat simpleDateFormat;
    //Hbase连接
    private Connection connection;
    //HBase表对象
    private Table table;
    //
    private List<Put> puts;
    //主被叫参数
    private String flag;

    public HBaseDao() throws IOException {
        //初始化相应的参数
        properties = PropertityUtil.getPropertity();
        nameSpace = properties.getProperty("hbase.namespace");
        tableName = properties.getProperty("hbase.table.name");
        regions = Integer.valueOf(properties.getProperty("hbase.regions.count"));
        cf = properties.getProperty("hbase.cf");
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        connection = ConnectionFactory.createConnection(Constant.CONF);
        table = connection.getTable(TableName.valueOf(tableName));
        puts = new ArrayList<Put>();
        flag="1";

        //初始化命名空间
        HBaseUtil.createNamespace(nameSpace);
        //创建表
        HBaseUtil.createTable(tableName, regions,cf,"f2");
    }

    //批量提交数据
    public void put(String value) throws ParseException, IOException {
        if (value==null){
            return;
        }

        //截取数据
        String[] split = value.split(",");
        String call1=split[0];
        String call2=split[1];
        String buildTime=split[2];
        String duration=split[3];

        Long buildTS = simpleDateFormat.parse(buildTime).getTime();
        //生成分区号
        String rowHash = HBaseUtil.getRowHash(regions, call1, buildTime);

        //生成rowkey
        String rowKey = HBaseUtil.getRowKey(rowHash, call1, buildTime, buildTS, call2, flag,duration);

        //生成put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call1"), Bytes.toBytes(call1));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildTS"), Bytes.toBytes(buildTS));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call2"), Bytes.toBytes(call2));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        puts.add(put);
        if (puts.size()>20){
            table.put(puts);
            puts.clear();
        }
    }

    public void close() throws IOException {
        table.put(puts);
        table.close();
        connection.close();
    }
}
