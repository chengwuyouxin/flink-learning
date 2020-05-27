package com.lpq.sql.source.mysql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class SourceFromMysql extends RichSourceFunction<Student> {
    private String server_ip = "192.168.29.149";
    private String db = "test";
    private String mysql_url = String.format(
            "jdbc:mysql://%s:3306/%s?useUnicode=true&characterEncoding=UTF-8"
            ,server_ip
            ,db);
    private String mysql_user = "root";
    private String mysql_passwd = "123456";
    private PreparedStatement ps;
    private Connection connection;

    public SourceFromMysql() {
        super();
    }


    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "select * from Student";
        if(connection != null){
            ps = this.connection.prepareStatement(sql);
        }
    }

    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if(connection != null){
            connection.close();
        }
        if(ps != null){
            ps.close();
        }
    }

    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        //只从mysql读取一次数据。如果想定时读取此处可以改为while循环
        ResultSet resultSet = ps.executeQuery();
        while(resultSet.next()){
            Student student = new Student(
                    resultSet.getInt("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("password").trim(),
                    resultSet.getInt("age")
            );
            sourceContext.collect(student);
        }
    }

    @Override
    public void cancel() {

    }

    private Connection getConnection(){
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection(mysql_url,mysql_user,mysql_passwd);
        } catch (Exception e) {
            System.out.println(String.format("Create mysql connection encourter exception:%s"
                    ,e.getMessage()));
        }
        return con;
    }

}
