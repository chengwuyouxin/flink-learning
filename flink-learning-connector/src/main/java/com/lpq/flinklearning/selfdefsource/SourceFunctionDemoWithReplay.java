package com.lpq.flinklearning.selfdefsource;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author liupengqiang
 * @date 2020/6/8
 *
 * 可重放的数据源，需要实现CheckpointedFunction接口
 */
public class SourceFunctionDemoWithReplay  implements SourceFunction<Long>, CheckpointedFunction {
    private volatile Boolean isRunning = true;
    //由于数据源是单线程执行，因此list中只有一个元素
    private transient ListState<Long> checkpointedCount;

    private Long count;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.checkpointedCount.clear();
        this.checkpointedCount.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.checkpointedCount = context
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<Long>("count",Long.class));
        /**
         * Returns true, if state was restored from the snapshot of a previous execution. This returns always false for
         * stateless tasks.
         */
        if(context.isRestored()){
            for(Long count : this.checkpointedCount.get()){
                this.count = count;
            }
        }
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while(isRunning && count < 1000){
            synchronized (ctx.getCheckpointLock()){
                ctx.collect(count);
                count++;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
