package com.lpq.stream.assigntimestampandwatermark;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * @author liupengqiang
 * @date 2020/6/8
 */
public class AssignInSourceFunction implements SourceFunction<Long> {
    private Long count = 0L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while(isRunning){
            ctx.collectWithTimestamp(count,System.currentTimeMillis());
            ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
            count++;
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
