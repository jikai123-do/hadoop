package com.iotek.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HbaseUtil {

    private static String tableName = "goods";
    private Configuration config = null;

    @Before
    public void initHbase(){
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "haitong-1:2181,haitong-2:2181,haitong-3:2181");
    }

    @Test
    public void createTable() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(config);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor family_base = new HColumnDescriptor("baseinfo");
        tableDescriptor.addFamily(family_base);
        HColumnDescriptor family_extend = new HColumnDescriptor("extendinfo");
        tableDescriptor.addFamily(family_extend);
        admin.createTable(tableDescriptor);
        admin.close();

    }

    @Test
    public void insertData() throws IOException {
        HTable table = new HTable(config,tableName);
        List<Put> puts = new ArrayList<>();

        Put put1 = new Put("rk00001".getBytes());
        put1.add("baseinfo".getBytes(), "name".getBytes(), "apple".getBytes());
        put1.add("baseinfo".getBytes(), "price".getBytes(), "12".getBytes());
        put1.add("baseinfo".getBytes(), "brand".getBytes(), "hongfushi".getBytes());

        put1.add("extendinfo".getBytes(), "color".getBytes(), "red".getBytes());

        Put put2 = new Put("rk00002".getBytes());
        put2.add("baseinfo".getBytes(), "name".getBytes(), "banana".getBytes());
        put2.add("baseinfo".getBytes(), "price".getBytes(), "6".getBytes());
        put2.add("baseinfo".getBytes(), "brand".getBytes(), "unknown".getBytes());

        put2.add("extendinfo".getBytes(), "color".getBytes(), "yellow".getBytes());

        puts.add(put1);
        puts.add(put2);

        table.put(puts);
        table.close();
    }

    @Test
    public void insertData2() throws IOException {
        HTable table = new HTable(config,tableName);
        List<Put> puts = new ArrayList<>();

        Put put1 = new Put("rk00003".getBytes());
        put1.add("baseinfo".getBytes(), "name".getBytes(), "orange".getBytes());
        put1.add("baseinfo".getBytes(), "price".getBytes(), "10".getBytes());
        put1.add("baseinfo".getBytes(), "brand".getBytes(), "xxx".getBytes());

        put1.add("extendinfo".getBytes(), "color".getBytes(), "yellow".getBytes());


        table.put(put1);
        table.close();
    }


    @Test
    public void updateData() throws IOException {
        HTable table = new HTable(config,tableName);

        Put put1 = new Put("rk00001".getBytes());
        put1.add("baseinfo".getBytes(), "name".getBytes(), "apple6".getBytes());


        table.put(put1);
        table.close();
    }

    @Test
    public void query() throws IOException {
        HTable table = new HTable(config,tableName);
        Get get = new Get("rk00001".getBytes());
        Result result = table.get(get);
        ;
        Result rowww = table.get(get);
        for (Cell cell : rowww.listCells()) {
            String row = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String qulisier = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            Long timestamp = cell.getTimestamp();

            System.out.println(row + "   " + family + ":" + qulisier + "->" + value + "    " + timestamp);
        }

    }

    @Test
    public void scan() throws IOException {
        HTable table = new HTable(config,tableName);

        //查询条件
        Scan scan = new Scan();
        scan.setStartRow("rk00001".getBytes());
        scan.setStopRow("rk00004".getBytes());
//        scan.addFamily("baseinfo".getBytes());
//        scan.addColumn("baseinfo".getBytes(), "name".getBytes());
//        scan.addColumn("baseinfo".getBytes(), "price".getBytes());

        //根据查看条件查该表，得到查询结果
        ResultScanner resultScan = table.getScanner(scan);
        showScanResult(resultScan);
    }

    private void showScanResult(ResultScanner resultScan) {
        System.out.println("*********************************************");
        for(Result result:resultScan){
            for(Cell cell:result.listCells()){
                String row = new String(CellUtil.cloneRow(cell));
                String family = new String(CellUtil.cloneFamily(cell));
                String qulisier = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));
                Long timestamp = cell.getTimestamp();

                System.out.println(row + "   " + family + ":" + qulisier + "->" + value + "    " + timestamp);
            }
        }
    }

    @Test
    public void delete() throws IOException {
        HTable table = new HTable(config,tableName);
        Delete del = new Delete("rk00003".getBytes());
        table.delete(del);
        table.close();
    }


}