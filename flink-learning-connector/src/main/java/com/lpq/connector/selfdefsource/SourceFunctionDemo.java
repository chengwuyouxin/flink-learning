package com.lpq.connector.selfdefsource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author liupengqiang
 * @date 2020/6/8
 *
 * 生成Long数据的数据源
 */
public class SourceFunctionDemo implements SourceFunction<Long> {
    private volatile Boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        long cnt = 0L;
        while(isRunning){
            cnt++;
            ctx.collect(cnt);
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        isRunning = true;
    }
}
