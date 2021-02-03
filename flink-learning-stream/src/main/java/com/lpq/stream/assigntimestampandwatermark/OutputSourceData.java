package com.lpq.stream.assigntimestampandwatermark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author liupengqiang
 * @date 2020/6/8
 */
public class OutputSourceData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Long> source = env.addSource(new AssignInSourceFunction());

        source.process(new MyProcessFunction()).print();

        env.execute("Long source!");
    }

    private static class MyProcessFunction
            extends ProcessFunction<Long, Tuple3<Long,String,String>> {

        @Override
        public void processElement(Long value, Context ctx, Collector<Tuple3<Long, String, String>> out) throws Exception {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd hh:MM:ss.SSS");
            String watermark = sdf.format(new Date(ctx.timerService().currentWatermark()));
            String processingTime = sdf.format(new Date(ctx.timerService().currentProcessingTime()));
            out.collect(Tuple3.of(value,watermark,processingTime));
        }
    }
}
