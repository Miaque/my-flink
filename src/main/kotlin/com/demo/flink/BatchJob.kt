package com.demo.flink

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val dataSource = env.fromElements(
        Tuple3.of(1, "张三", 12000.00),
        Tuple3.of(2, "李四", 22000.00),
        Tuple3.of(3, "王老五", 18000.00)
    )

    val dsp = dataSource.project<Tuple2<Double, String>>(2, 1)

    dsp.print()

    env.execute()
}