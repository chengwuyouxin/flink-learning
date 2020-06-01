package com.lpq.stream.transformation;



import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author liupengqiang
 * @date 2020/6/1
 *
 * Iterate 适用于迭代计算的场景，迭代计算就是利用结果进行循环计算
 * 数据迭代流向：DataStream -> IterateStream -> DataStream
 */

public class IterateExample {

    /**
     * 一个边界常量
     * 为后面的数据源（1）控制数据源个数（2）生成随机数的范围边界。（3）作为控制Fibonacci的迭代次数的边界
     */
    private static final int BOUND = 100;

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        //创建一个流运行环境，并设置缓存延迟时间为1毫秒
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setBufferTimeout(1);

        //设置流环境配置参数
        env.getConfig().setGlobalJobParameters(params);

        //创建一个数据流源，格式为是tuple2整数对
        DataStream<Tuple2<Integer, Integer>> inputStream;
        //判断main函数的参数是否包含input
        if (params.has("input")) {
            //如果包含则读取input的文件，把每一行转化为随机的整数对作为数据源
            inputStream = env.readTextFile(params.get("input")).map(new FibonacciInputMap());
        } else {
            System.out.println("Executing Iterate example with default input data set.");
            System.out.println("Use --input to specify file input.");
            //否则使用自定义数据源随机生成整数并发送元素
            inputStream = env.addSource(new RandomFibonacciSource());
        }

        /**
         *  把上面的数据流源转换成一个迭代数据流，如果5秒没有数据，迭代终止。
         *  InputMap作用：
         *  	例如：tuple2（3,17）转换成 tuple5(3,17,3,17,0)
         */
        IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> it = inputStream.map(new InputMap())
                .iterate(5000);


        /**
         * 将 迭代数据流转 经过  斐波那契数列函数的计算然后split 成一个 SplitStream
         * 斐波那契数列：是个数列，这个数列从第3项开始，每一项都等于前两项之和。
         * 将迭代的元素计算（step()）,然后根据边界条件（MySelector()）分成两条流（iterate和output流）
         * IterativeStream主要的两个方法filter或split分流操作任选其一进行迭代次数的逻辑控制。此处使用了split
         */
        SplitStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> step = it.map(new Step())
                .split(new MySelector());

        // close the iteration by selecting the tuples that were directed to the
        // 'iterate' channel in the output selector
        //选择iterate流继续上面的迭代流的操作
        it.closeWith(step.select("iterate"));

        // to produce the final output select the tuples directed to the
        // 'output' channel then get the input pairs that have the greatest iteration counter
        // on a 1 second sliding window
        //设置迭代完成的元素输出格式
//        DataStream<Tuple2<Tuple2<Integer, Integer>, Integer>> numbers = step.select("output")
//                .map(new OutputMap());
        DataStream<Tuple5<Integer,Integer,Integer,Integer,Integer>> numbers = step.select("output");

        //打印迭代完成元素
//        if (params.has("output")) {
//            numbers.writeAsText(params.get("output"));
//        } else {
//            System.out.println("Printing result to stdout. Use --output to specify output path.");
//            numbers.print();
//        }

        //由于默认的并行度是8，在文件夹下生成8个文件
        numbers.writeAsText("E://IterateExampleOutput");

        // execute the program
        env.execute("Streaming Iteration Example");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * 自定义数据源：每隔50毫秒发出一个数值范围（1,50）的Tuple2(Integer,Integer)元素，发送100次停止。
     * Generate BOUND number of random integer pairs from the range from 1 to BOUND/2.
     */
    private static class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        //随机函数
        private Random rnd = new Random();

        //数据源运行状态
        private volatile boolean isRunning = true;

        //用于判断发送元素次数
        private int counter = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
            //当数据源是运行状态并且发送元素次数少于100时，继续发送元素
            while (isRunning && counter < BOUND) {
                //tuple2的第一个值，随机生成（1,50）返回内的一个整数
                int first = rnd.nextInt(BOUND / 2 - 1) + 1;
                //tuple2的第二个值，随机生成（1,50）返回内的一个整数
                int second = rnd.nextInt(BOUND / 2 - 1) + 1;
                //数据源发送数据
                ctx.collect(new Tuple2<>(first, second));
                //判断发送次数+1
                counter++;
                //停顿50毫秒
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * 此方法没用到
     * Generate random integer pairs from the range from 0 to BOUND/2.
     */
    private static class FibonacciInputMap implements MapFunction<String, Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Integer, Integer> map(String value) throws Exception {
            String record = value.substring(1, value.length() - 1);
            String[] splitted = record.split(",");
            return new Tuple2<>(Integer.parseInt(splitted[0]), Integer.parseInt(splitted[1]));
        }
    }

    /**
     * 自定义的map方法：将tuple2转化为tuple5
     * 例如：将数据源发送古来的tuple2（3,17） 转为 tuple5（3,17,3,17,0）
     * 主要目的方便Fibonacci函数的计算和统计迭代次数
     * Map the inputs so that the next Fibonacci numbers can be calculated while preserving the original input tuple.
     * A counter is attached to the tuple and incremented in every iteration step.
     */
    public static class InputMap implements MapFunction<Tuple2<Integer, Integer>, Tuple5<Integer, Integer, Integer,
            Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) throws
                Exception {
            return new Tuple5<>(value.f0, value.f1, value.f0, value.f1, 0);
        }
    }

    /**
     * 真正执行Fibonacci函数的步骤：
     * 例如：tuple5（3,17,3,17,0）第一次迭代转化为：tuple5（3,17,3,20,1）返回
     * Iteration step function that calculates the next Fibonacci number.
     */
    public static class Step implements
            MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer,
                    Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple5<Integer, Integer, Integer, Integer,
                Integer> value) throws Exception {
            return new Tuple5<>(value.f0, value.f1, value.f2, value.f2 + value.f3, ++value.f4);
        }
    }

    /**
     * 分流的逻辑：
     * 根据Fibonacci函数算出来的结果：
     * 	<100的，分到iterate流，以便继续迭代
     * 	>=100的，分到output流，代表迭代完成，打印出来
     * OutputSelector testing which tuple needs to be iterated again.
     */
    public static class MySelector implements OutputSelector<Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Iterable<String> select(Tuple5<Integer, Integer, Integer, Integer, Integer> value) {
            List<String> output = new ArrayList<>();
            if (value.f2 < BOUND && value.f3 < BOUND) {
                output.add("iterate");
            } else {
                output.add("output");
            }
            return output;
        }
    }

    /**
     * 制定迭代完成元素的打印格式：tuple5 转为tuple2(tuple2,Integer)
     * tuple2代表原始元素，Integer代表迭代多少次，tuple2的两个值进行Fibonacci函数计算能超过100
     * Giving back the input pair and the counter.
     */
    public static class OutputMap implements MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>,
            Tuple2<Tuple2<Integer, Integer>, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Tuple2<Integer, Integer>, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer>
                                                                     value) throws
                Exception {
            return new Tuple2<>(new Tuple2<>(value.f0, value.f1), value.f4);
        }
    }

}
