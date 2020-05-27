package com.lpq.sql.source.mysql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink read data from Mysql
 */

public class MysqlSourceDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SourceFromMysql()).print();

        env.execute("Flink read data from mysql!");
    }

}

