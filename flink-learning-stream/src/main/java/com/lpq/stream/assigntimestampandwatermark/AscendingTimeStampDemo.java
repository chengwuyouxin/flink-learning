package com.lpq.stream.assigntimestampandwatermark;

import com.lpq.stream.transformation.sensor.SensorReading;
import com.lpq.stream.transformation.sensor.SensorSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author liupengqiang
 * @date 2020/6/9
 */
public class AscendingTimeStampDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> input = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
                    @Override
                    public long extractAscendingTimestamp(SensorReading element) {
                        return element.timestamp;
                    }
                });

        input.process(new MyProcessFunction()).print();

        env.execute();
    }
    private static class MyProcessFunction extends ProcessFunction<SensorReading, Tuple3<String,String,String>> {
        @Override
        public void processElement(SensorReading r, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
            String waterMark = sdf.format(new Date(ctx.timerService().currentWatermark()));
            String timeStamp = sdf.format(new Date(r.timestamp));
            out.collect(Tuple3.of(r.id,timeStamp,waterMark));

        }
    }
}
