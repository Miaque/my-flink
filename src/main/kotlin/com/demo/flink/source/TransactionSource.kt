package com.demo.flink.source

import com.demo.flink.entity.Transaction
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction
import java.io.Serializable

class TransactionSource() : FromIteratorFunction<Transaction>(RateLimitedIterator(TransactionIterator.unbounded())) {

    class RateLimitedIterator<T>(private val inner: Iterator<T>) : Iterator<T>, Serializable {

        override fun hasNext(): Boolean {
            return inner.hasNext()
        }

        override fun next(): T {
            try {
                Thread.sleep(100)
            } catch (e: Exception) {
                throw RuntimeException(e)
            }
            return inner.next()
        }

    }
}