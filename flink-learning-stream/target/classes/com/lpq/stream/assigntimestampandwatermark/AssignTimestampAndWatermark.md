 # Flink中使用TimeStamp和watermark
## SourceFunction中定义

## flink中已经实现的两种方式
   AscendingTimestampAssigner:
   提取记录中指定字段作为timestamp,使用当前timestamp作为watermark,适合于事件按升序顺序达到
   BoundedOutOfOrdernessTimestampExtractor :
   接受一个最大的数据延迟时间参数，适合于能够预测数据最大延迟的场景,
   生成的watermark比timestamp晚设定的最大容忍度

## 实现PeriodAssignerWaterMark接口
   周期性生成时间戳和水位线
   每次处理记录都会调用extractTimestamp产生记录的时间戳
   由env.getConfig().setAutoWatermarkInterval(2000L)来设置周期性调用getCurrentWatermark产生watermark
   
## 实现AssignerWithPunctuatedWatermarks接口  
   针对某一事件产生watermark
   每次处理记录时都要调用extractTimestamp提取事件戳，然后再调用checkAndGetNextWatermark，
   传入的参数包括记录和提取的记录事件戳