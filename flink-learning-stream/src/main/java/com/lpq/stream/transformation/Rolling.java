package com.lpq.stream.transformation;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liupengqiang
 * @date 2020/5/16
 *
    基于KeyedStream的转换
    sum() 滚动计算输入流中指定字段的和
    min() 滚动计算输入流中指定字段的最小值
    max() 滚动计算输入流中指定字段的最大值
    minBy() 滚动计算输入流中迄今为止的最小值，返回该值所在的事件
    maxBy() 滚动计算输入流中迄今为止的最大值，返回该值所在的事件
 */
public class Rolling {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<Integer,Integer,Integer>> inputStream =
                env.fromElements(
                        Tuple3.of(1,2,2),
                        Tuple3.of(2,3,1),
                        Tuple3.of(2,2,4),
                        Tuple3.of(1,5,3),
                        Tuple3.of(1,3,8),
                        Tuple3.of(2,5,10)
                );
        KeyedStream<Tuple3<Integer, Integer, Integer>, Tuple> keyed_Stream =
                inputStream.keyBy(0);


        /*
         * 输入元组按照第一个字段进行分区，然后滚动计算第二个字段的总和，第三个字段没有定义。
         * 本例中第三个字段一直输出每个分组的第一个元组的值
         * 键为1 先输出（1，2，2） 再输出（1，7，2）,再输出（1，10，2）
         * 键为2 先输出（2，3，1） 再输出（2，5，1），再输出（2，10，1）
         */
//        DataStream<Tuple3<Integer,Integer,Integer>> rollingsum =
//                keyed_Stream.sum(1);
//
//        rollingsum.print();

        //滚动计算输入流中迄今为止的最大值，返回该值所在的事件
        //返回位置1的最大值所在的元组
//        keyed_Stream.maxBy(1).print();

        //滚动计算输入流中指定字段的最大值
        //指定字段1返回最大值，位置3未定义
        keyed_Stream.max(1).print();

        env.execute();
    }
}
