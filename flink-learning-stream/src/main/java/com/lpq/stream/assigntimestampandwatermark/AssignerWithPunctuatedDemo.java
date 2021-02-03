package com.lpq.stream.assigntimestampandwatermark;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author liupengqiang
 * @date 2020/5/19
 *
 * AssignerWithPunctuatedWatermarks  针对某一事件产生watermark
 * 每次处理记录时都要调用extractTimestamp提取事件戳，然后再调用checkAndGetNextWatermark，
 * 传入的参数包括记录和提取的记录事件戳
 */
public class AssignerWithPunctuatedDemo{
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> input =
                env.addSource(new SensorSource())
                //配置时间戳并生成水位线
                .assignTimestampsAndWatermarks(new MyAssignerWithPunctuated());

        DataStream<Tuple3<String,String,String>> res =
                input.process(new MyProcessFunction());

        res.print();

        env.execute();
    }

    private static class MyAssignerWithPunctuated implements AssignerWithPunctuatedWatermarks<SensorReading> {

        Long bound= 1 * 1000L;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(SensorReading sensorReading, long extractedTimestamp) {
            if(sensorReading.id.equals("sensor_0")){
                return new Watermark(extractedTimestamp - bound);
            }else{
                return null;
            }
        }

        @Override
        public long extractTimestamp(SensorReading sensorReading, long previousTimeStamp) {
            return sensorReading.timestamp;
        }
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




