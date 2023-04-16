package com.demo.flink.operator

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val dataSource = env.setParallelism(1)
        .fromElements(-2L, 3L, 4L, 5L)

    val it = dataSource.iterate()

    val itBody = it.map { if (it < 0) it + 1 else it }

    val feedback = itBody.filter { it < 0 }

    it.closeWith(feedback)

    val output = itBody.filter { it >= 0 }

    output.print()

    env.execute("flink iterate transformation")
}