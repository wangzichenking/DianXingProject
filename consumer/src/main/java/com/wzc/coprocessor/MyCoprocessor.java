package com.wzc.coprocessor;


import com.wzc.Utils.HBaseUtil;
import com.wzc.Utils.PropertityUtil;
import com.wzc.constant.Constant;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class MyCoprocessor extends BaseRegionObserver {
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
                        Put put, WALEdit edit, Durability durability) throws IOException {

        //获得协处理器中的表
        String newTable = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
        //获取当前操作的表
        String oldTable = PropertityUtil.getPropertity().getProperty("hbase.table.name");

        if (newTable.equals(oldTable)){
            return;
        }

        //切分
        String rowKey = Bytes.toString(put.getRow());
        String[] split = rowKey.split("_");

        if ("0".equals(split[5])){
            return;
        }

        //获取所有的字段
        String caller = split[1];
        String buildTime = split[2];
        Long buildTS = Long.parseLong(split[3]);
        String callee = split[4];
        String duration = split[5];

        //生成新的分区号
        String rowHash = HBaseUtil.getRowHash(Integer.valueOf(PropertityUtil.getPropertity().getProperty("hbase.regions.count")), callee, buildTime);
        //生成新的rowkey
        String newRowKey = HBaseUtil.getRowKey(rowHash, callee, buildTime, buildTS, callee, "0", duration);
        //添加数据
        Put newPut = new Put(Bytes.toBytes(newRowKey));
        put.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(callee));
        put.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
        put.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTS"), Bytes.toBytes(buildTS));
        put.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(caller));
        put.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes("0"));
        put.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Table table = connection.getTable(TableName.valueOf(oldTable));

        table.put(put);

    }
}
