package com.lpq.sql.sink;

import com.lpq.sql.source.mysql.Student;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkDemoMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Student> inputdata = env.fromElements(
                new Student(1,"lpq","123",29),
                new Student(2,"xmm","234",31),
                new Student(3,"ljx","345",1)
        );
        inputdata.addSink(new Sink2Mysql());

        env.execute("Flink Sink data to mysql!");

    }

}
