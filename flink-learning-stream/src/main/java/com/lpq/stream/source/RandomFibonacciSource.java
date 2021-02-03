package com.lpq.stream.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author liupengqiang
 * @date 2021/1/26
 */
public class RandomFibonacciSource implements SourceFunction<Tuple2<Integer,Integer>> {

    private Random random =new Random();
    private volatile boolean isRunning=true;
    private int counter=0;
    private int MAX_RANDOM_VALUE=200;

    @Override
    public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
        while (isRunning) {
            int first = random.nextInt(MAX_RANDOM_VALUE / 2 - 1) + 1;
            int second = random.nextInt(MAX_RANDOM_VALUE / 2 -1) + 1;

            if (first > second){
                continue;
            }

            ctx.collect(new Tuple2<Integer, Integer>(first, second));
            counter++;
            Thread.sleep(50);
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
    }
}
