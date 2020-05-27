package com.lpq.stream.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author liupengqiang
 * @date 2020/5/20
 *
 * 基于window的join操作
 */
public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final long windowSize = params.getLong("windowSize",
                2000);
        final long rate = params.getLong("rate",3);

        System.out.println("Using windowSize=" + windowSize + ", data rate=" + rate);
        System.out.println("To customize example, use: WindowJoin [--windowSize <window-size-in-millis>] " +
                "[--rate <elements-per-second>]");

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Tuple2<String,Integer>> grades =
                WindowJoinSampleData.GradeSource.getSource(env,rate);
        DataStream<Tuple2<String,Integer>> salaries =
                WindowJoinSampleData.SalarySource.getSource(env,rate);

        DataStream<Tuple3<String,Integer,Integer>> joinedStream =
                grades.join(salaries)
                .where(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> values) throws Exception {
                        return values.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> values) throws Exception {
                        return values.f0;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
                        return Tuple3.of(v1.f0,v1.f1,v2.f1);
                    }
                });

        joinedStream.print().setParallelism(1);

        env.execute("windowed join Example");

    }
}
