package com.lpq.stream.transformation;

import com.lpq.stream.transformation.sensor.SensorReading;
import com.lpq.stream.transformation.sensor.SensorSource;
import com.lpq.stream.transformation.sensor.SensorTimeAssigner;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author liupengqiang
 * @date 2020/5/16
 *
 * 基本的转换操作 map/filter/flatMap
 * 对应的基本函数有 MapFunction/FilterFunction/FlatMapFunction
 * 对应的Rich function 有RichMapFunction/RichFilterFunction/RichFlatMapFunction
 * 利用Rich function获取RuntimeContext，由此获取函数并行度，子任务编号，执行函数任务名称
 * ，分区状态信息
 *
 * int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
 * String taskname = getRuntimeContext().getTaskName();
 * int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
 */
public class BasicTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> reading =
                env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<String> res = reading
//                .filter(r -> r.temperature >= 25)
                .filter(new FilterFunction<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading sensorReading) throws Exception {
                        return sensorReading.temperature >= 25;
                    }
                })
//                .map( r -> r.id)
                .map(new MapFunction<SensorReading, String>() {
                    @Override
                    public String map(SensorReading sensorReading) throws Exception {
                        return sensorReading.id;
                    }
                })
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        for(String str : s.split(" ")){
                            collector.collect(str);
                        }
                    }
                })
                .returns(Types.STRING);

        res.print();

        env.execute("Basic Transformations Example!");


    }
}
