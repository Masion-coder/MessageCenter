package com.messagecenter;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MySQL {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    // static final String DB_URL = "jdbc:mysql://localhost:3306/RUNOOB";

    // MySQL 8.0 以上版本 - JDBC 驱动名及数据库 URL
    // static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static private final String MYSQL_URL = "jdbc:mysql://localhost:3306/";
    private String DB_URL;

    static final String PARAMETER = "useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

    // 数据库的用户名与密码，需要根据自己的设置
    final String USER;
    final String PASS;

    private String database;
    private String table;

    private Connection conn;
    private Statement stmt;

    private ThreadPoolExecutor pool;

    class Task implements Runnable {
        Connection connection;
        PreparedStatement statement;
        
        public Task(Connection connection, PreparedStatement statement) {
            this.connection = connection;
            this.statement = statement;
        }

        @Override
        public void run() {
            try {
                statement.executeBatch();
                statement.close();
                connection.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public MySQL(String userName, String password) throws SQLException {
        this.USER = userName;
        this.PASS = password;
        pool = new ThreadPoolExecutor(8, 8, 0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(256), new ThreadPoolExecutor.DiscardPolicy());
    }

    public void setDatabase(String database) {
        this.database = database;
        DB_URL = MYSQL_URL + this.database + "?" + PARAMETER;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public boolean connect() {
        try {
            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void close() throws SQLException {
        if (conn != null)
            conn.close();
        if (stmt != null)
            stmt.close();
    }

    public int insert(List<Message> set) throws SQLException {
        if (set.isEmpty())
            return 0;
        Connection connection = DriverManager.getConnection(DB_URL, USER, PASS);
        PreparedStatement statement = connection.prepareStatement("INSERT INTO message(serialNumber, time, tag, value) VALUES(?, ?, ?, ?)");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        int n = 0;
        int m = 0;
        for (Message message : set) {
            n++;
            if (n / 5000 > m) {
                pool.execute(new Task(connection, statement));
                connection = DriverManager.getConnection(DB_URL, USER, PASS);
                statement = connection.prepareStatement("INSERT INTO message(serialNumber, time, tag, value) VALUES(?, ?, ?, ?)");
                System.out.println("插入:" + n);
                m++;
            }
            try {
                statement.setLong(1, message.serialNumber);
                statement.setString(2, message.time == null ? "NULL" : df.format(message.time));
                statement.setString(3, message.tag);
                statement.setString(4, MAPPER.writeValueAsString(message.value));
                statement.addBatch();
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (n != 0) {
            statement.executeBatch();
        }
        statement.close();
        connection.close();

        return set.size();
    }

    public ResultSet query(String sql) throws SQLException {
        return stmt.executeQuery(sql);
    }

    public ResultSet query(int limit) throws SQLException {
        return this.query(String.format(
                "SELECT * FROM (SELECT * FROM message ORDER BY time DESC LIMIT %d) AS temp ORDER BY time ASC;", limit));
    }

    public ResultSet query(String tag, int limit) throws SQLException {
        return this.query(String.format(
                "SELECT * FROM (SELECT * FROM message WHERE tag = %s ORDER BY time DESC LIMIT %d) AS temp ORDER BY time ASC;",
                tag, limit));
    }

    public long getSerialNumber() throws SQLException {
        ResultSet rs = this.query(String.format(
                "SELECT * FROM serialNumber;"));
        if (rs.next()) {
            return rs.getLong(1);
        } else {
            return 0;
        }
    }
}
