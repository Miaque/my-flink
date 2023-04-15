package com.demo.flink.operator

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val dataSource = env.fromElements(Tuple2.of("good", 1), Tuple2.of("good", 2), Tuple2.of("study", 1))

    dataSource.keyBy { it.f0 }
        .sum(1)
        .print()

    dataSource.keyBy { it.f0 }
        .min(1)
        .print()

    dataSource.keyBy { it.f0 }
        .max(1)
        .print()

    dataSource.keyBy { it.f0 }
        .minBy(1)
        .print()

    dataSource.keyBy { it.f0 }
        .maxBy(1)
        .print()

    env.execute("flink aggregation transformation")
}