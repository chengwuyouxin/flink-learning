package com.lpq.stream.assigntimestampandwatermark;

import com.lpq.stream.transformation.sensor.SensorReading;
import com.lpq.stream.transformation.sensor.SensorSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
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
 * AssignerWithPeriodicWatermarks 周期性生成时间戳和水位线
 * 每次处理记录都会调用extractTimestamp产生记录的时间戳
 * 由env.getConfig().setAutoWatermarkInterval(2000L)来设置周期性调用getCurrentWatermark产生watermark
 */
public class AssignerWithPeriodicDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //使用AssignerWithPeriodWaterMarks时需要设置
        //相当于每2s产生一次水位线，水位线由AssignerWithPeriodWaterMarks.getCurrentWaterMark()产生
        env.getConfig().setAutoWatermarkInterval(2000L);

        DataStream<SensorReading> input =
                env.addSource(new SensorSource())
                //配置时间戳并生成水位线
                .assignTimestampsAndWatermarks(new MyAssignerWithPeriodic());

        DataStream<Tuple3<String,String,String>> res =
                input.process(new MyProcessFunction());

        res.print();

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

    private static class MyAssignerWithPeriodic implements AssignerWithPeriodicWatermarks<SensorReading>{
        //迟到超过10s的记录会作为迟到记录
        //从输出中可以看到，每次处理的记录时间戳比watermark快10秒
        Long bound = 10 * 1000L;
        Long maxTs = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            //当前最新的时间戳 减去 容忍度，即迟到超过bound的记录会作为迟到记录处理
            return new Watermark(maxTs - bound);
        }


        /**
         * @param sensorReading
         * @param timestamp  上次记录的时间戳
         * @return
         */
        @Override
        public long extractTimestamp(SensorReading sensorReading, long timestamp) {
            maxTs = Math.max(maxTs,sensorReading.timestamp);
            return sensorReading.timestamp;
        }
    }
}



