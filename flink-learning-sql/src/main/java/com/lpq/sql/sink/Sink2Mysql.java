package com.lpq.sql.sink;

import com.lpq.sql.source.mysql.Student;
import org.apache.calcite.prepare.Prepare;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Sink2Mysql extends RichSinkFunction<Student> {
    private PreparedStatement ps;
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = getConnection();
        String sql = "insert into Student(id,name,password,age) value (?,?,?,?);";
        if(conn != null){
            ps = this.conn.prepareStatement(sql);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(conn != null){
            conn.close();
        }
        if(ps != null){
            ps.close();
        }
    }

    @Override
    public void invoke(Student value, Context context) throws Exception {
        if(ps == null){
            return;
        }
        ps.setInt(1,value.getId());
        ps.setString(2,value.getName());
        ps.setString(3,value.getPassword());
        ps.setInt(4,value.getAge());
        ps.executeUpdate();
    }

    private Connection getConnection(){
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(
                    "jdbc:mysql://192.168.29.149:3306/test?useUnicode=true&characterEncoding=UTF-8",
                    "root",
                    "123456");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
}
