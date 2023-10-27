package com.example.es;

import java.sql.*;

public class TestJdbc {
    public static void main(String[] args) {

        try {
            //1创建连接
            Connection connection = DriverManager.getConnection("jdbc:es://http://localhost:9200");

            //2创建statement
            Statement statement = connection.createStatement();

            //3执行sql
            ResultSet resultSet = statement.executeQuery("select * from tvs");    //SQL語句

            //4解析结果
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1));
                System.out.println(resultSet.getString(2));
                System.out.println(resultSet.getString(3));
                System.out.println(resultSet.getString(4));
                System.out.println("=================================");

            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
