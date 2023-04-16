package com.demo.flink.partition

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val dataSource = env.fromElements(1, 2, 3, 4, 5, 6)

    dataSource.rebalance().print("rebalance").setParallelism(2)

    env.execute("rebalance分区示例")
}