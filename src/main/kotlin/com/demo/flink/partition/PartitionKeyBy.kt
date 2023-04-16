package com.demo.flink.partition

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val dataSource = env.fromElements(1, 2, 3, 4, 5, 6)

    val operator = dataSource.map { Tuple2.of(it % 3, it) }
        .returns(Types.TUPLE(Types.INT, Types.INT))
        .setParallelism(2)

    operator.keyBy { it.f0 }
        .print("key")

    env.execute("keyby分区示例")
}