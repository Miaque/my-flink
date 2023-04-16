package com.demo.flink.partition

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val dataSource = env.fromElements(1, 2, 3, 4, 5)

    dataSource.map { it * it }
        .setParallelism(2)
        .forward()
        .print("forward")
        .setParallelism(2)

    env.execute("forward分区示例")
}