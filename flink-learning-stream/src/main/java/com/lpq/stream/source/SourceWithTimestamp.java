package com.lpq.stream.source;

import com.lpq.stream.source.sensor.SensorReading;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author liupengqiang
 * @date 2021/1/29
 */
public class SourceWithTimestamp extends RichParallelSourceFunction<SensorReading> {

    private volatile boolean isRunning=true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        Random rand = new Random();
        // look up index of this parallel task
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();
        String sensorId = "sensor_"+taskIdx;

        while (isRunning) {
            // get current time
            long curTime = Calendar.getInstance().getTimeInMillis();
            double curTemp = 65 + rand.nextGaussian() * 20;
            ctx.collectWithTimestamp(new SensorReading(sensorId, curTime, curTemp),curTime);

            // wait for 100 ms
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
    }
}
