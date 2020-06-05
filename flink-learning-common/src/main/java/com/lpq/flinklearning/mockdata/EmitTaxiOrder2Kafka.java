package com.lpq.flinklearning.mockdata;


import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * @author liupengqiang
 * @date 2020/6/5
 */
public class EmitTaxiOrder2Kafka {
    public static void main(String[] args) {
        int corePoolSize = 4;
        int maxPoolSize = 100;
        Long keepAliveTime = 10L;
        TimeUnit timeUnit = TimeUnit.SECONDS;
        BlockingQueue<Runnable> blockingQueue =
                new ArrayBlockingQueue<>(2);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            corePoolSize,maxPoolSize,keepAliveTime,timeUnit,blockingQueue
        );

        for(int i = 0; i<4; i++){
            executor.execute(new EmitTaxiDataTask());
        }
    }
}
