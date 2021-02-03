package com.lpq.stream.function;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liupengqiang
 * @date 2020/5/20
 *
 * RichFunction 可以获取 RuntimeContext，得到上下文的信息
 */
public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<SensorReading> input =
                env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());
        DataStream<Tuple4<Long,String,Integer,Integer>> result =
                input.map(new RichMapFunction<SensorReading, Tuple4<Long,String, Integer, Integer>>() {
                    @Override
                    public Tuple4<Long,String, Integer, Integer> map(SensorReading r) throws Exception {
                        Long timeStamp = r.timestamp;
                        int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
                        String taskname = getRuntimeContext().getTaskName();
                        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
                        return Tuple4.of(timeStamp,taskname,subTaskIndex,parallelism);
                    }
                });
        input.print().setParallelism(1);
        env.execute("Rich function demo！");
    }
}
