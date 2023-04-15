package com.demo.flink.source

import com.demo.flink.entity.Transaction
import java.io.Serializable
import java.sql.Timestamp

class TransactionIterator(private var bounded: Boolean) : Iterator<Transaction>, Serializable {

    private var timestamp: Long = INITIAL_TIMESTAMP.time
    private var index = 0

    companion object {
        @JvmField
        val INITIAL_TIMESTAMP: Timestamp = Timestamp.valueOf("2020-01-01 00:00:00")

        const val SIX_MINUTES = 6 * 60 * 1000

        @JvmField
        val data: List<Transaction> = arrayListOf(
            Transaction(1L, 0L, 188.23),
            Transaction(2L, 0L, 374.79),
            Transaction(3L, 0L, 112.15)
        )
        @JvmStatic
        fun bounded(): TransactionIterator {
            return TransactionIterator(true)
        }
        @JvmStatic
        fun unbounded(): TransactionIterator {
            return TransactionIterator(false)
        }
    }

    override fun hasNext(): Boolean {
        return if (index < data.size) {
            true
        } else if (!bounded) {
            index = 0
            true
        } else {
            false
        }
    }

    override fun next(): Transaction {
        val transaction = data[index++]
        transaction.timestamp = timestamp
        timestamp += SIX_MINUTES
        return transaction
    }
}