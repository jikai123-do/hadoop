package com.iotek.hive;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class HiveUtil {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://192.168.47.128:10000/gj";   //mydb用户自己的数据库
    private static String userName="root";
    private static String pwd = "root";

    private static Connection connection = null;
    private static Statement statement = null;
    private static ResultSet resultSet = null;

    @Before
    public void initDB(){
        System.setProperty("hadoop.home.dir","F:\\software\\hadoop-2.6.4");
        try {
            Class.forName(driverName);
            connection = DriverManager.getConnection(url, userName, pwd);
            statement = connection.createStatement();
        } catch (Exception e) {
            System.out.println(e.toString());
        }

    }

    @Test
    public void createDB() throws SQLException {
        String sql = "create database haitong_hive1";
        statement.execute(sql);

    }

    @Test
    public void showdatabases() throws SQLException {
        String sql = "show databases";
        resultSet = statement.executeQuery(sql);
        while(resultSet.next()){
            System.out.println(resultSet.getString(1));
        }
    }

    @Test
    public void indatabases() throws SQLException {
        String sql = "use haitong_hive1";
        statement.execute(sql);
    }

    @Test
    public void createNewTable() throws SQLException {
        indatabases();
        String sql = "create table orders123( orderid int, goodsid int, numb int ) row format delimited fields terminated by ' '";
        statement.execute(sql);
    }

    @Test
    public void showTables() throws SQLException {
        indatabases();
        String sql = "show tables";
        resultSet = statement.executeQuery(sql);
        while(resultSet.next()){
            System.out.println(resultSet.getString(1));
        }
    }

    @Test
    public void loadData() throws SQLException {
        indatabases();
        String sql = "load data local inpath '/home/hadoop/orders.txt' overwrite into table orders123";
        statement.execute(sql);
    }

    @Test
    public void queryData() throws SQLException {
        indatabases();
        String sql = "select * from orders123";
        resultSet = statement.executeQuery(sql);
        while(resultSet.next()){
            System.out.println("" + resultSet.getInt(1));
            System.out.println("" + resultSet.getInt(2));
            System.out.println("" + resultSet.getInt(3));
            System.out.println("-------------------");
        }
    }

    @Test
    public void showTableStruct() throws SQLException {
        indatabases();
        String sql = "desc orders123";
        resultSet = statement.executeQuery(sql);
        while(resultSet.next()){
            System.out.println(resultSet.getString(1) + ":" + resultSet.getString(2));

        }

    }

    @Test
    public void dropTable() throws SQLException {
        indatabases();
        String sql = "drop table orders123";
        statement.execute(sql);
    }

    @After
    public void releaseResource() throws SQLException {
        if(resultSet != null){
            resultSet.close();
            resultSet = null;
        }

        if(statement != null){
            statement.close();
            statement = null;
        }

        if(connection != null){
            connection.close();
            connection = null;
        }


    }



}