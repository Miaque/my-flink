package com.demo.flink.partition

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val dataSource = env.fromElements(1, 2, 3, 4, 5)

    dataSource.broadcast().print("broadcast").setParallelism(2)

    env.execute("broadcast分区示例")
}