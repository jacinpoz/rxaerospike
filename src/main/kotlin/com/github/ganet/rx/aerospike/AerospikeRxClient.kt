/*
 * Copyright 2016 Jose Ignacio Acin Pozo. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.ganet.rx.aerospike


import com.aerospike.client.AerospikeException
import com.aerospike.client.BatchRead
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.Language
import com.aerospike.client.Operation
import com.aerospike.client.Record
import com.aerospike.client.Value
import com.aerospike.client.async.AsyncClient
import com.aerospike.client.async.IAsyncClient
import com.aerospike.client.listener.BatchListListener
import com.aerospike.client.listener.BatchSequenceListener
import com.aerospike.client.listener.DeleteListener
import com.aerospike.client.listener.ExecuteListener
import com.aerospike.client.listener.ExistsArrayListener
import com.aerospike.client.listener.ExistsListener
import com.aerospike.client.listener.ExistsSequenceListener
import com.aerospike.client.listener.RecordArrayListener
import com.aerospike.client.listener.RecordListener
import com.aerospike.client.listener.RecordSequenceListener
import com.aerospike.client.listener.WriteListener
import com.aerospike.client.policy.BatchPolicy
import com.aerospike.client.policy.Policy
import com.aerospike.client.policy.QueryPolicy
import com.aerospike.client.policy.ScanPolicy
import com.aerospike.client.policy.WritePolicy
import com.aerospike.client.query.IndexCollectionType
import com.aerospike.client.query.IndexType
import com.aerospike.client.query.Statement
import io.reactivex.BackpressureStrategy
import io.reactivex.BackpressureStrategy.MISSING
import io.reactivex.Completable
import io.reactivex.Flowable
import io.reactivex.Single

/**
 * Created by JoseIgnacio on 09/11/2016.
 */
class AerospikeRxClient(val client: AsyncClient) : IAsyncClient by client {

    /* AsyncClient Async Operations */
    fun rxAsyncPut(policy: WritePolicy?, key: Key, vararg bins: Bin): Single<Key> {
        return Single.create {
            client.put(policy, object : WriteListener {
                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }

                override fun onSuccess(key: Key?) {
                    it.onSuccess(key)
                }

            }, key, *bins)
        }
    }

    fun rxAsyncAppend(policy: WritePolicy?, key: Key, vararg bins: Bin): Single<Key> {
        return Single.create {
            client.append(policy, object : WriteListener {
                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }

                override fun onSuccess(key: Key?) {
                    it.onSuccess(key)
                }

            }, key, *bins)
        }
    }

    fun rxAsyncPrepend(policy: WritePolicy?, key: Key, vararg bins: Bin): Single<Key> {
        return Single.create {
            client.prepend(policy, object : WriteListener {
                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }

                override fun onSuccess(key: Key?) {
                    it.onSuccess(key)
                }

            }, key, *bins)
        }
    }

    fun rxAsyncAdd(policy: WritePolicy?, key: Key, vararg bins: Bin): Single<Key> {
        return Single.create {
            client.add(policy, object : WriteListener {
                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }

                override fun onSuccess(key: Key?) {
                    it.onSuccess(key)
                }

            }, key, *bins)
        }
    }

    fun rxAsyncDelete(policy: WritePolicy?, key: Key): Single<Pair<Key, Boolean>> {
        return Single.create {
            client.delete(policy, object : DeleteListener {
                override fun onSuccess(key: Key, success: Boolean) {
                    // Nice wordplay :)
                    it.onSuccess(key to success)
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }

            }, key)
        }
    }

    fun rxAsyncTouch(policy: WritePolicy?, key: Key): Single<Key> {
        return Single.create {
            client.touch(policy, object : WriteListener {
                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }

                override fun onSuccess(key: Key?) {
                    it.onSuccess(key)
                }
            }, key)
        }
    }

    fun rxAsyncExists(policy: Policy?, key: Key): Single<Pair<Key, Boolean>> {
        return Single.create {
            client.exists(policy, object : ExistsListener {
                override fun onSuccess(key: Key, success: Boolean) {
                    // Nice wordplay :)!
                    it.onSuccess(key to success)
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, key)
        }
    }

    fun rxAsyncExists(policy: BatchPolicy?, keys: Array<Key>): Single<Pair<Array<Key>, BooleanArray>> {
        return Single.create {
            client.exists(policy, object : ExistsArrayListener {
                override fun onSuccess(keyArray: Array<Key>, booleanArray: BooleanArray) {
                    it.onSuccess(keyArray to booleanArray)
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, keys)
        }
    }

    @JvmOverloads
    fun rxAsyncExistsSequence(policy: BatchPolicy?, keys: Array<Key>, backPressure: BackpressureStrategy = MISSING): Flowable<Pair<Key, Boolean>> {
        return Flowable.create({
            client.exists(policy, object : ExistsSequenceListener {
                override fun onSuccess() {
                    it.onComplete()
                }

                override fun onExists(key: Key, exist: Boolean) {
                    it.onNext(key to exist)
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, keys)
        }, backPressure)
    }

    fun rxAsyncGet(policy: Policy?, key: Key): Single<Pair<Key, Record?>> {
        return Single.create {
            client.get(policy, object : RecordListener {
                override fun onSuccess(key: Key, record: Record?) {
                    it.onSuccess(key to record)
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, key)
        }
    }

    fun rxAsyncGet(policy: Policy?, key: Key, vararg binNames: String): Single<Pair<Key, Record?>> {
        return Single.create {
            client.get(policy, object : RecordListener {
                override fun onSuccess(key: Key, record: Record?) {
                    it.onSuccess(key to record)
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, key, *binNames)
        }
    }

    fun rxAsyncGetHeader(policy: Policy?, key: Key): Single<Pair<Key, Record?>> {
        return Single.create {
            client.getHeader(policy, object : RecordListener {
                override fun onSuccess(key: Key, record: Record?) {
                    it.onSuccess(key to record)
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, key)
        }
    }

    fun rxAsyncGet(policy: BatchPolicy?, records: List<BatchRead>): Single<List<BatchRead>> {
        return Single.create {
            client.get(policy, object : BatchListListener {
                override fun onSuccess(list: MutableList<BatchRead>?) {
                    it.onSuccess(list)
                }


                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, records)
        }
    }

    @JvmOverloads
    fun rxAsyncGetSequence(policy: BatchPolicy?, records: List<BatchRead>, backPressure: BackpressureStrategy = MISSING): Flowable<BatchRead> {
        return Flowable.create({
            client.get(policy, object : BatchSequenceListener {
                override fun onRecord(read: BatchRead?) {
                    it.onNext(read)
                }

                override fun onSuccess() {
                    it.onComplete()
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, records)
        }, backPressure)
    }

    fun rxAsyncGet(policy: BatchPolicy?, keys: Array<Key>): Single<Pair<Array<Key>, Array<Record?>>> {
        return Single.create {
            client.get(policy, object : RecordArrayListener {
                override fun onSuccess(keyArray: Array<Key>, records: Array<Record?>) {
                    it.onSuccess(keyArray to records)
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, keys)
        }
    }

    @JvmOverloads
    fun rxAsyncGetSequence(policy: BatchPolicy?, keys: Array<Key>, backPressure: BackpressureStrategy = MISSING): Flowable<Pair<Key, Record?>> {
        return Flowable.create({
            client.get(policy, object : RecordSequenceListener {
                override fun onRecord(key: Key, record: Record?) {
                    it.onNext(key to record)
                }

                override fun onSuccess() {
                    it.onComplete()
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, keys)
        }, backPressure)
    }

    fun rxAsyncGet(policy: BatchPolicy?, keys: Array<Key>, vararg binNames: String): Single<Pair<Array<Key>, Array<Record?>>> {
        return Single.create {
            client.get(policy, object : RecordArrayListener {
                override fun onSuccess(keyArray: Array<Key>, records: Array<Record?>) {
                    it.onSuccess(keyArray to records)
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, keys, *binNames)
        }
    }

    @JvmOverloads
    fun rxAsyncGetSequence(policy: BatchPolicy?, keys: Array<Key>, vararg binNames: String, backPressure: BackpressureStrategy = MISSING): Flowable<Pair<Key, Record?>> {
        return Flowable.create({
            client.get(policy, object : RecordSequenceListener {
                override fun onRecord(key: Key, record: Record?) {
                    it.onNext(key to record)
                }

                override fun onSuccess() {
                    it.onComplete()
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, keys, *binNames)
        }, backPressure)
    }

    fun rxAsyncGetHeader(policy: BatchPolicy?, keys: Array<Key>): Single<Pair<Array<Key>, Array<Record?>>> {
        return Single.create {
            client.getHeader(policy, object : RecordArrayListener {
                override fun onSuccess(keyArray: Array<Key>, records: Array<Record?>) {
                    it.onSuccess(keyArray to records)
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, keys)
        }
    }

    @JvmOverloads
    fun rxAsyncGetHeaderSequence(policy: BatchPolicy?, keys: Array<Key>, backPressure: BackpressureStrategy = MISSING): Flowable<Pair<Key, Record?>> {
        return Flowable.create({
            client.getHeader(policy, object : RecordSequenceListener {
                override fun onRecord(key: Key, record: Record?) {
                    it.onNext(key to record)
                }

                override fun onSuccess() {
                    it.onComplete()
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, keys)
        }, backPressure)
    }


    fun rxAsyncOperate(policy: WritePolicy?, key: Key, vararg operations: Operation): Single<Pair<Key, Record?>> {
        return Single.create {
            client.operate(policy, object : RecordListener {
                override fun onSuccess(key: Key, record: Record?) {
                    it.onSuccess(key to record)
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, key, *operations)
        }
    }

    @JvmOverloads
    fun rxAsyncScanAll(policy: ScanPolicy?, namespace: String, setName: String, vararg binNames: String, backPressure: BackpressureStrategy = MISSING): Flowable<Pair<Key, Record?>> {
        return Flowable.create({
            client.scanAll(policy, object : RecordSequenceListener {
                override fun onRecord(key: Key, record: Record?) {
                    it.onNext(key to record)
                }

                override fun onSuccess() {
                    it.onComplete()
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, namespace, setName, *binNames)
        }, backPressure)
    }

    fun rxAsyncExecute(policy: WritePolicy?, key: Key, packageName: String, functionName: String, vararg functionArgs: Value): Single<Pair<Key, Any?>> {
        return Single.create {
            client.execute(policy, object : ExecuteListener {
                override fun onSuccess(key: Key, obj: Any?) {
                    it.onSuccess(key to obj)
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, key, packageName, functionName, *functionArgs)
        }
    }

    @JvmOverloads
    fun rxAsyncQuery(policy: QueryPolicy?, statement: Statement, backPressure: BackpressureStrategy = MISSING): Flowable<Pair<Key, Record?>> {
        return Flowable.create({
            client.query(policy, object : RecordSequenceListener {
                override fun onRecord(key: Key, record: Record?) {
                    it.onNext(key to record)
                }

                override fun onSuccess() {
                    it.onComplete()
                }

                override fun onFailure(exception: AerospikeException?) {
                    it.onError(exception)
                }
            }, statement)
        }, backPressure)
    }
    /* End of Async Operations */

    /* Register/Create/Execute operations. This operations need to be polled, so ideally you should observe on background threads. */

    @JvmOverloads
    fun rxRegister(policy: Policy?, clientPath: String, serverPath: String, language: Language, sleepInterval: Int = 1000): Completable {
        return Completable.create {
            try {
                client.register(policy, clientPath, serverPath, language).waitTillComplete(sleepInterval)
                it.onComplete()
            } catch(e: RuntimeException) {
                it.onError(e)
            }
        }
    }

    @JvmOverloads
    fun rxRegister(policy: Policy?, resourceLoader: ClassLoader, resourcePath: String, serverPath: String, language: Language, sleepInterval: Int = 1000): Completable {
        return Completable.create {
            try {
                client.register(policy, resourceLoader, resourcePath, serverPath, language).waitTillComplete(sleepInterval)
                it.onComplete()
            } catch(e: RuntimeException) {
                it.onError(e)
            }
        }
    }

    @JvmOverloads
    fun rxRegisterUdfString(policy: Policy?, code: String, serverPath: String, language: Language, sleepInterval: Int = 1000): Completable {
        return Completable.create {
            try {
                client.registerUdfString(policy, code, serverPath, language).waitTillComplete(sleepInterval)
                it.onComplete()
            } catch(e: RuntimeException) {
                it.onError(e)
            }
        }
    }

    @JvmOverloads
    fun rxExecute(
            policy: WritePolicy?,
            statement: Statement,
            packageName: String,
            functionName: String,
            sleepInterval: Int = 1000,
            vararg functionArgs: Value): Completable {
        return Completable.create {
            try {
                client.execute(policy, statement, packageName, functionName, *functionArgs).waitTillComplete(sleepInterval)
                it.onComplete()
            } catch(e: RuntimeException) {
                it.onError(e)
            }
        }

    }

    @JvmOverloads
    fun rxCreateIndex(
            policy: Policy?,
            namespace: String,
            setName: String?,
            indexName: String,
            binName: String,
            indexType: IndexType,
            sleepInterval: Int = 1000): Completable {
        return Completable.create {
            try {
                client.createIndex(policy, namespace, setName, indexName, binName, indexType).waitTillComplete(sleepInterval)
                it.onComplete()
            } catch(e: RuntimeException) {
                it.onError(e)
            }
        }

    }

    @JvmOverloads
    fun rxCreateIndex(
            policy: Policy?,
            namespace: String,
            setName: String?,
            indexName: String,
            binName: String,
            indexType: IndexType,
            indexCollectionType: IndexCollectionType,
            sleepInterval: Int = 1000): Completable {
        return Completable.create {
            try {
                client.createIndex(policy, namespace, setName, indexName, binName, indexType, indexCollectionType).waitTillComplete(sleepInterval)
                it.onComplete()
            } catch(e: RuntimeException) {
                it.onError(e)
            }
        }
    }

    /* End of Register/Create/Execute operations */
}
