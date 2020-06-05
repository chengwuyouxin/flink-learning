package com.lpq.sql.source.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class KafkaTableSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnvironment =
                StreamTableEnvironment.create(env,settings);

        String ddlSource = "create table order_table (" +
                "name String," +
                "product String," +
                "amount int," +
                "price double" +
                " ) with (" +
                " 'connector.type' = 'kafka'," +
                " 'connector.version' = '0.11'," +
                " 'connector.topic' = 'order'," +
                " 'connector.startup-mode' = 'latest-offset'," +
                " 'connector.properties.zookeeper.connect' = '192.168.29.149:2181'," +
                " 'connector.properties.bootstrap.servers' = '192.168.29.149:9092'," +
                " 'format.type' = 'json'" +
                ")";

        String countSql = "select name,sum(amount) from order_table group by name";

        blinkStreamTableEnvironment.sqlUpdate(ddlSource);

        Table result = blinkStreamTableEnvironment.sqlQuery(countSql);

        blinkStreamTableEnvironment.toRetractStream(result, Row.class).print();

        blinkStreamTableEnvironment.execute("Flink read data from kafka table");
    }
}
