package com.example.mighouse.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class DataMigrationService {

    public boolean migrateData(String sourceDbType, String sourceHost, String sourcePort, String sourceDatabase, String sourceUser, String sourcePassword, String clickHouseHost, String clickHousePort, String clickHouseDatabase, String clickHouseUser, String clickHousePassword, String sourceTable, String clickHouseTable) {
        try {
            // 连接源数据库
            Connection sourceConn = connectToSourceDatabase(sourceDbType, sourceHost, sourcePort, sourceDatabase, sourceUser, sourcePassword);

            // 连接 ClickHouse 数据库
            Connection clickHouseConn = connectToClickHouse(clickHouseHost, clickHousePort, clickHouseDatabase, clickHouseUser, clickHousePassword);

            // 迁移数据
            migrateData(sourceConn, clickHouseConn, sourceTable, clickHouseTable);

            // 关闭连接
            sourceConn.close();
            clickHouseConn.close();

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private static Connection connectToSourceDatabase(String dbType, String host, String port, String database, String user, String password) throws SQLException {
        String url;

        if ("oracle".equalsIgnoreCase(dbType)) {
            url = "jdbc:oracle:thin:@" + host + ":" + port + ":" + database;
        } else if ("mysql".equalsIgnoreCase(dbType)) {
            url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useSSL=false&serverTimezone=UTC";
        } else {
            throw new IllegalArgumentException("不支持的数据库类型: " + dbType);
        }

        return DriverManager.getConnection(url, user, password);
    }

    private static Connection connectToClickHouse(String host, String port, String database, String user, String password) throws SQLException {
        String url = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        return DriverManager.getConnection(url, user, password);
    }

    private static void migrateData(Connection sourceConn, Connection clickHouseConn, String sourceTable, String clickHouseTable) throws SQLException {
        Statement sourceStmt = sourceConn.createStatement();
        ResultSet sourceResultSet = sourceStmt.executeQuery("SELECT * FROM " + sourceTable);

        ResultSetMetaData sourceMetaData = sourceResultSet.getMetaData();
        int columnCount = sourceMetaData.getColumnCount();

        Statement clickHouseStmt = clickHouseConn.createStatement();
        StringBuilder insertQuery = new StringBuilder("INSERT INTO " + clickHouseTable + " VALUES ");

        while (sourceResultSet.next()) {
            insertQuery.append("(");
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    insertQuery.append(",");
                }

                String columnValue = sourceResultSet.getString(i);
                insertQuery.append((columnValue != null && !columnValue.isEmpty()) ? "'" + columnValue.replace("'", "''") + "'" : "NULL");
            }
            insertQuery.append("),");
        }

        if (sourceResultSet.getRow() > 0) {
            insertQuery.setLength(insertQuery.length() - 1); // 移除最后一个逗号
            clickHouseStmt.executeUpdate(insertQuery.toString());
            System.out.println("数据已成功迁移。");
        } else {
            System.out.println("源表没有数据。");
        }

        sourceResultSet.close();
        sourceStmt.close();
        clickHouseStmt.close();
    }
}
