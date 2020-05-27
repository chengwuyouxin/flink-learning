package com.lpq.stream.transformation.connect;

import com.lpq.stream.transformation.sensor.SensorReading;
import com.lpq.stream.transformation.sensor.SensorSource;
import com.lpq.stream.transformation.sensor.SensorTimeAssigner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author liupengqiang
 * @date 2020/5/26
 * Connect算子用于将两个流进行连接，但是两个流中的事件不会产生任何关联，所有事件随机
 * 分发到各个算子实例，因此产生的结果是不确定的。
 * 一般使用方法是先对流进行keyBy操作，然后相同键值的记录分配到相同的算子上，
 * 有先分组再连接和先连接再分组两种方式。
 *
 * Connect两个流产生ConnectedStreams，其上可以应用的方法有
 * 1.CoMapFunction
 * 2.CoFlatMapFunction
 * 3.CoProcessFunction
 */
public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<SensorReading> input1 = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<Tuple2<String,Long>> input2 = env.fromElements(
                Tuple2.of("sensor_1", 10 * 1000L),
                Tuple2.of("sensor_2", 60 * 1000L));

        //
        //先连接再分组
        ConnectedStreams<SensorReading,Tuple2<String,Long>> connectedStreams =
                input1.connect(input2);
        ConnectedStreams<SensorReading,Tuple2<String,Long>> keyedConnected =
                connectedStreams.keyBy(r -> r.id, s -> s.f0);
        //先分组再连接
//        ConnectedStreams<SensorReading,Tuple2<String,Long>> KeyedConnected =
//                input1.keyBy(r -> r.id).connect(input2.keyBy(s->s.f0));


        //注意！！！没有关联上的结果此时也在ConnectedStream中
        //进入到flatMap的算子，两个流中关联上的记录，stream1中的记录在flatMap1中处理，
        //stream2中的记录在flatMap2方法中处理，可以在处理函数中对关联的记录进行处理
        //两个流中未关联上的记录，进入各自的方法中进行处理
        keyedConnected.flatMap(new CoFlatMapFunction<SensorReading, Tuple2<String, Long>,
                Tuple2<String,String>>() {

            private String filterid = null;
            @Override
            public void flatMap1(SensorReading r, Collector<Tuple2<String, String>> out) throws Exception {
                out.collect(Tuple2.of(r.id,this.filterid));
            }

            @Override
            public void flatMap2(Tuple2<String, Long> t, Collector<Tuple2<String, String>> out) throws Exception {
                this.filterid = t.f0;
            }
        }).print();

        env.execute("Two Stream Connect CoMapfunction");

    }

}
