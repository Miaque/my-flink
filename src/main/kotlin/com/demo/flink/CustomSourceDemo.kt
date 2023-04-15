package com.demo.flink

import com.demo.flink.source.TransactionSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

fun main(args: Array<String>) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    env.addSource(TransactionSource())
        .name("transactions")
        .print()

    env.execute("Transaction Stream")
}