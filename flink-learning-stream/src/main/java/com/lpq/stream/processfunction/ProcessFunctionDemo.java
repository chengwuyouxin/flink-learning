package com.lpq.stream.processfunction;

import com.lpq.stream.transformation.sensor.SensorReading;
import com.lpq.stream.transformation.sensor.SensorSource;
import com.lpq.stream.transformation.sensor.SensorTimeAssigner;
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
 * @date 2020/5/20
 *
 * ProcessFunction 是一组相对底层的转换，除了基本的操作外，可以
 * 1.访问记录的时间戳/水位线/当前处理时间
 * ctx.timerService().currentWatermark();
 * r.timestamp
 * ctx.timerService().currentProcessingTime()
 *
 * 2.注册和删除计时器  KeyedProcessFunctionDemo
 * context.timerService().registerEventTimeTimer(timeTs);
 * context.timerService().deleteEventTimeTimer(currentTimeStamp);
 *
 * 3.side output  SideOutputDemo
 *
 */
public class ProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //控制水位线的时间间隔，每2秒调用一次AssignerWithPeriodWaterMarks.getCurrentWaterMark()
        //相当于每2s产生一次水位线，水位线的具体时间则由AssignerWithPeriodWaterMarks或PunctunatedWaterMark控制
        env.getConfig().setAutoWatermarkInterval(2000L);

        DataStream<SensorReading> input =
                env.addSource(new SensorSource())
                //配置时间戳并生成水位线
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<Tuple3<String,String,String>> res =
                input.process(new MyProcessFunction());

        res.print();

        env.execute("ProcessFunction Demo!");
    }

    private static class MyProcessFunction extends ProcessFunction<SensorReading, Tuple3<String,String,String>> {

        @Override
        public void processElement(SensorReading r, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
            String waterMark = sdf.format(new Date(ctx.timerService().currentWatermark()));
            String processingTime = sdf.format(new Date(ctx.timerService().currentProcessingTime()));
            String timeStamp = sdf.format(new Date(r.timestamp));
            out.collect(Tuple3.of(r.id,timeStamp,waterMark));

        }
    }
}

