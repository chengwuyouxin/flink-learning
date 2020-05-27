package com.lpq.sql.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class SQLStreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv =
                StreamTableEnvironment.create(env,settings);

        String filePath = SQLStreamWordCount.class.getClassLoader()
                .getResource("words.txt").getPath();
        CsvTableSource csvTableSource = CsvTableSource.builder()
                .field("word", Types.STRING)
                .path(filePath)
                .build();

        blinkStreamTableEnv.registerTableSource("wordstable",csvTableSource);
        Table result = blinkStreamTableEnv.sqlQuery(
                "select word,count(1) from wordstable group by word");
        blinkStreamTableEnv
                .toRetractStream(result, Row.class).print();
        blinkStreamTableEnv.execute("Stream wordcount with blink planner!");



    }
}
