package com.demo.flink.source

import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.runtime.state.FunctionInitializationContext
import org.apache.flink.runtime.state.FunctionSnapshotContext
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction

class ExampleCountSource : SourceFunction<Long>, CheckpointedFunction {
    private var count: Long = 0
    @Volatile private var isRunning: Boolean = true
    @Transient private lateinit var checkpointedCount: ListState<Long>

    override fun run(ctx: SourceFunction.SourceContext<Long>) {
        while (isRunning && count < 1000) {
            synchronized(ctx.checkpointLock) {
                ctx.collect(count)
                count++
            }
        }
    }

    override fun cancel() {
        this.isRunning = false
    }

    override fun snapshotState(context: FunctionSnapshotContext) {
        this.checkpointedCount.clear()
        this.checkpointedCount.add(count)
    }

    override fun initializeState(context: FunctionInitializationContext) {
        this.checkpointedCount = context.operatorStateStore.getListState(ListStateDescriptor("count", Long::class.java))

        if (context.isRestored) {
            for (count in this.checkpointedCount.get()) {
                this.count = count
            }
        }
    }
}