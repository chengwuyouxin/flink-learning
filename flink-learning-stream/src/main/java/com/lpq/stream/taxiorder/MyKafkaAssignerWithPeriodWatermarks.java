package com.lpq.stream.taxiorder;

import com.lpq.flinklearning.dao.TaxiOrder;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author liupengqiang
 * @date 2020/6/8
 */
public class MyKafkaAssignerWithPeriodWatermarks implements AssignerWithPeriodicWatermarks<TaxiOrder> {

    Long bound = 0 * 1000L;
    Long maxTs = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTs - bound);
    }

    @Override
    public long extractTimestamp(TaxiOrder element, long previousElementTimestamp) {
        maxTs = Math.max(element.getOrderTime(),maxTs);
        return element.getOrderTime();
    }
}
