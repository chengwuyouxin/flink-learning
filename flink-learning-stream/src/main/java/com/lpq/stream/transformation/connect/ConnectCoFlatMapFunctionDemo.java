package com.lpq.stream.transformation.connect;

import com.lpq.stream.source.sensor.SensorReading;
import com.lpq.stream.source.sensor.SensorSource;
import com.lpq.stream.source.sensor.SensorTimeAssigner;
import com.lpq.stream.transformation.smoke.Alert;
import com.lpq.stream.transformation.smoke.SmokeLevel;
import com.lpq.stream.transformation.smoke.SmokeLevelSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author liupengqiang
 * @date 2020/5/16
 */
public class ConnectCoFlatMapFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> tempReadings = env
                .addSource(new SensorSource())
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        DataStream<SmokeLevel> smokeReadings = env
                .addSource(new SmokeLevelSource())
                .setParallelism(1);

        KeyedStream<SensorReading,String> keyedTempReadings = tempReadings
                .keyBy(r -> r.id);

        ConnectedStreams<SensorReading,SmokeLevel> connectedStreams =
                keyedTempReadings.connect(smokeReadings.broadcast());

        DataStream<Alert> alerts = keyedTempReadings
                .connect(smokeReadings.broadcast())
                //对关联上记录，通过smokeLevel变量使二者产生关联进行过滤
                //flatMap1处理stream1中的记录 flatMap2处理stream2中的记录
                .flatMap(new CoFlatMapFunction<SensorReading, SmokeLevel, Alert>() {
                    private SmokeLevel smokeLevel = SmokeLevel.LOW;
                    @Override
                    public void flatMap1(SensorReading sensorReading, Collector<Alert> collector) throws Exception {
                        if(this.smokeLevel == SmokeLevel.HIGH
                        && sensorReading.temperature > 100){
                            collector.collect(new Alert(
                                    "Risk of fire! "+sensorReading,sensorReading.timestamp));
                        }
                    }

                    @Override
                    public void flatMap2(SmokeLevel smokeLevel, Collector<Alert> collector) throws Exception {
                        this.smokeLevel = smokeLevel;
                    }
                });

        alerts.print();

        env.execute("Multi-Stream Transformation Example!");
    }
}
