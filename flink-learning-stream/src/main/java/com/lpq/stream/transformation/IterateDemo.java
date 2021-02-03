package com.lpq.stream.transformation;

import com.lpq.stream.source.RandomFibonacciSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liupengqiang
 * @date 2021/1/26
 *
 * DataSteam -> IterativeStream ->DataStream
 * 该算子常用于迭代计算，经过IterateStream
 *
 *
 */
public class IterateDemo {
    public static void main(String[] args) throws Exception {
//        //获取输入参数
//        ParameterTool params= ParameterTool.fromArgs(args);
//        //创建执行环境
//        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
//        //设置输入参数
//        env.getConfig().setGlobalJobParameters(params);



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf=new Configuration();
        conf.setInteger(RestOptions.PORT,8088);
        env.getConfig().setGlobalJobParameters(conf);



        //输入是一个随机产生的两元素元组
        DataStream<Tuple2<Integer,Integer>> input= env.addSource(new RandomFibonacciSource());
        //转换成一个五元素元组
        DataStream<Tuple5<Integer,Integer,Integer,Integer,Integer>> fibonacciStream=
                input.map(new MapFunction<Tuple2<Integer, Integer>, Tuple5<Integer,Integer,Integer,Integer,Integer>>() {

                    @Override
                    public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                        return Tuple5.of(value.f0,value.f1,value.f0,value.f1,0);
                    }
                });

        //DataStream -> IterativeStream  此处的5000只最大等待时间，当超过5000ms没有需要迭代的数据到来时停止迭代
        IterativeStream<Tuple5<Integer,Integer,Integer,Integer,Integer>> iterativeStream =
                fibonacciStream.iterate(5000);

        //IterativeStream -> DataStream 对数据进行迭代处理，f2+f3相加，保留上次的f3
        DataStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> stepedStream=iterativeStream.map(new RichMapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>,
                Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer> value) throws Exception {
                return Tuple5.of(value.f0,
                        value.f1,
                        value.f3,
                        value.f2+value.f3,
                        ++value.f4);
            }
        });


        //对处理过的数据进行分流，通过filter将符合条件的元组直接输出
        int StopNum=1000;
        DataStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> output=
                stepedStream.filter(new FilterFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple5<Integer, Integer, Integer, Integer, Integer> value) throws Exception {
                return value.f3>StopNum;
            }
        });

        //通过filter将不符合条件的元素通过 IterativeStream.closeWith()方法重新去进行业务操作的迭代
        DataStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> feedback=
                stepedStream.filter(new FilterFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple5<Integer, Integer, Integer, Integer, Integer> value) throws Exception {
                        return value.f3<=StopNum ;
                    }
                });
        iterativeStream.closeWith(feedback);

        output.print();

        env.execute();

    }
}
