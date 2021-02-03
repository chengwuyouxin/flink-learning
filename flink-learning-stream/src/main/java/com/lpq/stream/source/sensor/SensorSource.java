/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lpq.stream.source.sensor;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * Flink SourceFunction to generate SensorReadings with random temperature values.
 *
 * Each parallel instance of the source simulates 10 sensors which emit one sensor reading every 100 ms.
 *
 * 默认启动CPU线程数量个线程，每个线程都会产生10个sensor的数据
 * 在此电脑中启动8个线程产生数据，每个线程随机生产10个sensor的数据，编号从 sensor_0  到 sensor_79
 *
 * Note: This is a simple data-generating source function that does not checkpoint its state.
 * In case of a failure, the source does not replay any data.
 */
public class SensorSource extends RichParallelSourceFunction<SensorReading> {

    // flag indicating whether source is still running
    private boolean running = true;

    /** run() continuously emits SensorReadings by emitting them through the SourceContext. */
    @Override
    public void run(SourceContext<SensorReading> srcCtx) throws Exception {

        /*
        // initialize random number generator
        Random rand = new Random();
        // look up index of this parallel task
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();

        // initialize sensor ids and temperatures
        String[] sensorIds = new String[10];
        double[] curFTemp = new double[10];
        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + (taskIdx * 10 + i);
            //nextGaussian() 产生数值服从正态分布
            curFTemp[i] = 65 + (rand.nextGaussian() * 20);
        }

        while (running) {

            // get current time
            long curTime = Calendar.getInstance().getTimeInMillis();

            // emit SensorReadings
            for (int i = 0; i < 10; i++) {
                // update current temperature
                curFTemp[i] += rand.nextGaussian() * 0.5;
                // emit reading
                srcCtx.collect(new SensorReading(sensorIds[i], curTime, curFTemp[i]));
            }

            // wait for 100 ms
            Thread.sleep(1000);
        }
        */

        // 修改后每个传感器每秒钟产生一条数据
        Random rand = new Random();
        // look up index of this parallel task
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();
        String sensorId = "sensor_"+taskIdx;

        while (running) {

            // get current time
            long curTime = Calendar.getInstance().getTimeInMillis();
            double curTemp = 65 + rand.nextGaussian() * 20;
            srcCtx.collect(new SensorReading(sensorId, curTime, curTemp));

            // wait for 100 ms
            Thread.sleep(1000);
        }
    }

    /** Cancels this SourceFunction. */
    @Override
    public void cancel() {
        this.running = false;
    }
}
