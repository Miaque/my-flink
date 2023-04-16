package com.demo.flink.partition

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val dataSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)

    dataSource.print("before rescale")

    dataSource.rescale().print("rescale").setParallelism(2)

    env.execute("rescale分区示例")
}