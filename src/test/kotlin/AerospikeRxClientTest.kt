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

import com.aerospike.client.AerospikeException
import com.aerospike.client.BatchRead
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.Language
import com.aerospike.client.Operation
import com.aerospike.client.Record
import com.aerospike.client.Value
import com.aerospike.client.async.AsyncClient
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
import com.aerospike.client.policy.QueryPolicy
import com.aerospike.client.policy.ScanPolicy
import com.aerospike.client.policy.WritePolicy
import com.aerospike.client.query.IndexCollectionType
import com.aerospike.client.query.IndexType
import com.aerospike.client.query.Statement
import com.aerospike.client.task.ExecuteTask
import com.aerospike.client.task.IndexTask
import com.aerospike.client.task.RegisterTask
import com.github.ganet.rx.aerospike.AerospikeRxClient
import com.github.ganet.rx.aerospike.data.AerospikeArrayResult
import com.github.ganet.rx.aerospike.data.AerospikeResult
import org.junit.Before
import org.junit.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito.`when`
import org.mockito.Mockito.doNothing
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify


/**
 * Created by JoseIgnacio on 17/11/2016.
 */
class AerospikeRxClientTest {
    val policy = WritePolicy()
    val batchPolicy = BatchPolicy()
    val scanPolicy = ScanPolicy()
    val queryPolicy = QueryPolicy()
    val namespace = "testNamespace"
    val setName = "testSet"
    val indexName = "testIndexName"
    val indexType = IndexType.STRING
    val indexCollectionType = IndexCollectionType.LIST
    val clientPath = "clientPath"
    val serverPath = "serverPath"
    val language = Language.LUA
    val classLoader = ClassLoader.getSystemClassLoader()
    val packageName = "testPackage"
    val functionName = "testFunction"
    val key = Key(namespace, setName, "test")
    val keyArray = arrayOf(key)
    val booleanArray = booleanArrayOf(true)
    val binName = "TestBin"
    val bin = Bin(binName, 2.0)
    val listOfBatchRead = listOf(BatchRead(key, true), BatchRead(key, false), BatchRead(key, true))
    val record = Record(emptyMap(), 1, 1)
    val recordArray : Array<Record?> = arrayOf(record)
    val exception = AerospikeException()
    val statement = Statement()

    val value = Value.getAsNull()
    val registerTask = mock(RegisterTask::class.java)
    val executeTask = mock(ExecuteTask::class.java)
    val indexTask = mock(IndexTask::class.java)

    val operations = arrayOf(Operation(Operation.Type.ADD, binName, Value.get(2.0)),
            Operation(Operation.Type.APPEND, "Hello", Value.get("World")),
            Operation(Operation.Type.READ, "World", Value.getAsNull()))


    var writeListenerCaptor = ArgumentCaptor.forClass(WriteListener::class.java)
    var deleteListenerCaptor = ArgumentCaptor.forClass(DeleteListener::class.java)

    var existsListenerCaptor = ArgumentCaptor.forClass(ExistsListener::class.java)
    var existsArrayListenerCaptor = ArgumentCaptor.forClass(ExistsArrayListener::class.java)
    var existsSequenceListenerCaptor = ArgumentCaptor.forClass(ExistsSequenceListener::class.java)

    var executeListenerCaptor = ArgumentCaptor.forClass(ExecuteListener::class.java)

    var batchListListener =  ArgumentCaptor.forClass(BatchListListener::class.java)
    var batchSequenceListener =  ArgumentCaptor.forClass(BatchSequenceListener::class.java)

    var recordListener = ArgumentCaptor.forClass(RecordListener::class.java)
    var recordArrayListener =  ArgumentCaptor.forClass(RecordArrayListener::class.java)
    var recordSequenceListener =  ArgumentCaptor.forClass(RecordSequenceListener::class.java)

    var mockClient = mock(AsyncClient::class.java)
    var rxClient = AerospikeRxClient(mockClient)

    @Before
    fun initialiseMockVariables() {
        mockClient = mock(AsyncClient::class.java)
        rxClient = AerospikeRxClient(mockClient)
    }

    @Test
    fun testRxAsyncPutSuccess() {
        val singlePut = rxClient.rxAsyncPut(policy, key, bin).test()
        verify(mockClient).put(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onSuccess(key)
        singlePut.assertResult(key)
    }

    @Test
    fun testRxAsyncPutFailure() {
        val singlePut = rxClient.rxAsyncPut(policy, key, bin).test()
        verify(mockClient).put(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onFailure(exception)
        singlePut.assertError(exception)
    }

    @Test
    fun testRxAsyncAppendSuccess() {
        val singleAppend = rxClient.rxAsyncAppend(policy, key, bin).test()
        verify(mockClient).append(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onSuccess(key)
        singleAppend.assertResult(key)
    }

    @Test
    fun testRxAsyncAppedFailure() {
        val singleAppend = rxClient.rxAsyncAppend(policy, key, bin).test()
        verify(mockClient).append(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onFailure(exception)
        singleAppend.assertError(exception)
    }


    @Test
    fun testRxAsyncPrependSuccess() {
        val singlePrepend = rxClient.rxAsyncPrepend(policy, key, bin).test()
        verify(mockClient).prepend(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onSuccess(key)
        singlePrepend.assertResult(key)
    }

    @Test
    fun testRxAsyncPrependFailure() {
        val singlePrepend = rxClient.rxAsyncPrepend(policy, key, bin).test()
        verify(mockClient).prepend(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onFailure(exception)
        singlePrepend.assertError(exception)
    }

    @Test
    fun testRxAsyncAddSuccess() {
        val singleAdd = rxClient.rxAsyncAdd(policy, key, bin).test()
        verify(mockClient).add(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onSuccess(key)
        singleAdd.assertResult(key)
    }

    @Test
    fun testRxAsyncAddFailure() {
        val singleAdd = rxClient.rxAsyncAdd(policy, key, bin).test()
        verify(mockClient).add(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onFailure(exception)
        singleAdd.assertError(exception)
    }

    @Test
    fun testRxAsyncDeleteSuccess() {
        val singleDelete = rxClient.rxAsyncDelete(policy, key).test()
        verify(mockClient).delete(eq(policy), deleteListenerCaptor.capture(), eq(key))
        deleteListenerCaptor.value.onSuccess(key, true)
        singleDelete.assertResult(AerospikeResult(key, true))
    }

    @Test
    fun testRxAsyncDeleteFailure() {
        val singleDelete = rxClient.rxAsyncDelete(policy, key).test()
        verify(mockClient).delete(eq(policy), deleteListenerCaptor.capture(), eq(key))
        deleteListenerCaptor.value.onFailure(exception)
        singleDelete.assertError(exception)
    }

    @Test
    fun testRxAsyncTouchSuccess() {
        val singleTouch = rxClient.rxAsyncTouch(policy, key).test()
        verify(mockClient).touch(eq(policy), writeListenerCaptor.capture(), eq(key))
        writeListenerCaptor.value.onSuccess(key)
        singleTouch.assertResult(key)
    }

    @Test
    fun testRxAsyncTouchFailure() {
        val singleTouch = rxClient.rxAsyncTouch(policy, key).test()
        verify(mockClient).touch(eq(policy), writeListenerCaptor.capture(), eq(key))
        writeListenerCaptor.value.onFailure(exception)
        singleTouch.assertError(exception)
    }

    @Test
    fun testRxAsyncExistsSuccess() {
        val singleExists = rxClient.rxAsyncExists(policy, key).test()
        verify(mockClient).exists(eq(policy), existsListenerCaptor.capture(), eq(key))
        existsListenerCaptor.value.onSuccess(key, true)
        singleExists.assertResult(AerospikeResult(key, true))
    }

    @Test
    fun testRxAsyncExistsFailure() {
        val singleExists = rxClient.rxAsyncExists(policy, key).test()
        verify(mockClient).exists(eq(policy), existsListenerCaptor.capture(), eq(key))
        existsListenerCaptor.value.onFailure(exception)
        singleExists.assertError(exception)
    }

    @Test
    fun testRxAsyncExistsArraySuccess() {
        val singleExists = rxClient.rxAsyncExists(batchPolicy, keyArray).test()
        verify(mockClient).exists(eq(batchPolicy), existsArrayListenerCaptor.capture(), eq(keyArray))
        existsArrayListenerCaptor.value.onSuccess(keyArray, booleanArray)
        singleExists.assertResult(AerospikeArrayResult(keyArray, booleanArray))
    }

    @Test
    fun testRxAsyncExistsArrayFailure() {
        val singleExists = rxClient.rxAsyncExists(batchPolicy, keyArray).test()
        verify(mockClient).exists(eq(batchPolicy), existsArrayListenerCaptor.capture(), eq(keyArray))
        existsArrayListenerCaptor.value.onFailure(exception)
        singleExists.assertError(exception)
    }

    @Test
    fun testRxAsyncExistsSequenceSuccess() {
        val flowableExists = rxClient.rxAsyncExistsSequence(batchPolicy, keyArray).test()
        verify(mockClient).exists(eq(batchPolicy), existsSequenceListenerCaptor.capture(), eq(keyArray))
        existsSequenceListenerCaptor.value.onExists(key, true)
        existsSequenceListenerCaptor.value.onExists(key, false)
        existsSequenceListenerCaptor.value.onExists(key, true)
        existsSequenceListenerCaptor.value.onSuccess()
        flowableExists.assertResult(AerospikeResult(key, true), AerospikeResult(key, false), AerospikeResult(key, true))
    }

    @Test
    fun testRxAsyncExistsSequenceFailure() {
        val flowableExists = rxClient.rxAsyncExistsSequence(batchPolicy, keyArray).test()
        verify(mockClient).exists(eq(batchPolicy), existsSequenceListenerCaptor.capture(), eq(keyArray))
        existsSequenceListenerCaptor.value.onExists(key, true)
        existsSequenceListenerCaptor.value.onExists(key, false)
        existsSequenceListenerCaptor.value.onExists(key, true)
        existsSequenceListenerCaptor.value.onFailure(exception)
        flowableExists.assertValues(AerospikeResult(key, true), AerospikeResult(key, false), AerospikeResult(key, true))
        flowableExists.assertError(exception)
    }

    @Test
    fun testRxAsyncGetSuccess() {
        val singleGet = rxClient.rxAsyncGet(policy, key).test()
        verify(mockClient).get(eq(policy), recordListener.capture(), eq(key))
        recordListener.value.onSuccess(key, record)
        singleGet.assertResult(AerospikeResult(key, record))
    }

    @Test
    fun testRxAsyncGetFailure() {
        val singleGet = rxClient.rxAsyncGet(policy, key).test()
        verify(mockClient).get(eq(policy), recordListener.capture(), eq(key))
        recordListener.value.onFailure(exception)
        singleGet.assertError(exception)
    }

    @Test
    fun testRxAsyncGetBinsSuccess() {
        val singleGetBins = rxClient.rxAsyncGet(policy, key, binName).test()
        verify(mockClient).get(eq(policy), recordListener.capture(), eq(key), eq(binName))
        recordListener.value.onSuccess(key, record)
        singleGetBins.assertResult(AerospikeResult(key, record))
    }

    @Test
    fun testRxAsyncGetBinsFailure() {
        val singletGetBins = rxClient.rxAsyncGet(policy, key, binName).test()
        verify(mockClient).get(eq(policy), recordListener.capture(), eq(key), eq(binName))
        recordListener.value.onFailure(exception)
        singletGetBins.assertError(exception)
    }

    @Test
    fun testRxAsyncGetHeaderSuccess() {
        val singleGetHeader = rxClient.rxAsyncGetHeader(policy, key).test()
        verify(mockClient).getHeader(eq(policy), recordListener.capture(), eq(key))
        recordListener.value.onSuccess(key, record)
        singleGetHeader.assertResult(AerospikeResult(key, record))
    }

    @Test
    fun testRxAsyncGetHeaderFailure() {
        val singleGetHeader = rxClient.rxAsyncGetHeader(policy, key).test()
        verify(mockClient).getHeader(eq(policy), recordListener.capture(), eq(key))
        recordListener.value.onFailure(exception)
        singleGetHeader.assertError(exception)
    }

    @Test
    fun testRxAsyncGetBatchReadSuccess() {
        val singleGetBatchRead = rxClient.rxAsyncGet(batchPolicy, listOfBatchRead).test()
        verify(mockClient).get(eq(batchPolicy), batchListListener.capture(), eq(listOfBatchRead))
        batchListListener.value.onSuccess(listOfBatchRead)
        singleGetBatchRead.assertResult(listOfBatchRead)
    }

    @Test
    fun testRxAsyncGetBatchReadFailure() {
        val singleGetBatchRead = rxClient.rxAsyncGet(batchPolicy, listOfBatchRead).test()
        verify(mockClient).get(eq(batchPolicy), batchListListener.capture(), eq(listOfBatchRead))
        batchListListener.value.onFailure(exception)
        singleGetBatchRead.assertError(exception)
    }

    @Test
    fun testRxAsyncGetSequenceBatchListSuccess() {
        val flowableGetSequenceBatchList = rxClient.rxAsyncGetSequence(batchPolicy, listOfBatchRead).test()
        verify(mockClient).get(eq(batchPolicy), batchSequenceListener.capture(), eq(listOfBatchRead))
        batchSequenceListener.value.onRecord(listOfBatchRead[0])
        batchSequenceListener.value.onRecord(listOfBatchRead[1])
        batchSequenceListener.value.onRecord(listOfBatchRead[2])
        batchSequenceListener.value.onSuccess()
        flowableGetSequenceBatchList.assertResult(*listOfBatchRead.toTypedArray())
    }

    @Test
    fun testRxAsyncGetSequenceBatchListFailure() {
        val flowableGetSequenceBatchList = rxClient.rxAsyncGetSequence(batchPolicy, listOfBatchRead).test()
        verify(mockClient).get(eq(batchPolicy), batchSequenceListener.capture(), eq(listOfBatchRead))
        batchSequenceListener.value.onRecord(listOfBatchRead[0])
        batchSequenceListener.value.onRecord(listOfBatchRead[1])
        batchSequenceListener.value.onRecord(listOfBatchRead[2])
        batchSequenceListener.value.onFailure(exception)
        flowableGetSequenceBatchList.assertValues(*listOfBatchRead.toTypedArray())
        flowableGetSequenceBatchList.assertError(exception)
    }

    @Test
    fun testRxAsyncGetBatchReadArrayKeySuccess() {
        val singleGetBatchArrayKeyRead = rxClient.rxAsyncGet(batchPolicy, keyArray).test()
        verify(mockClient).get(eq(batchPolicy), recordArrayListener.capture(), eq(keyArray))
        recordArrayListener.value.onSuccess(keyArray, recordArray)
        singleGetBatchArrayKeyRead.assertResult(AerospikeArrayResult(keyArray, recordArray))
    }

    @Test
    fun testRxAsyncGetBatchReadArrayKeyFailure() {
        val singleGetBatchArrayKeyRead = rxClient.rxAsyncGet(batchPolicy, keyArray).test()
        verify(mockClient).get(eq(batchPolicy), recordArrayListener.capture(), eq(keyArray))
        recordArrayListener.value.onFailure(exception)
        singleGetBatchArrayKeyRead.assertError(exception)
    }

    @Test
    fun testRxAsyncGetSequenceArrayKeySuccess() {
        val flowableGetSequenceArrayKey = rxClient.rxAsyncGetSequence(batchPolicy, keyArray).test()
        verify(mockClient).get(eq(batchPolicy), recordSequenceListener.capture(), eq(keyArray))
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onSuccess()
        flowableGetSequenceArrayKey.assertResult(AerospikeResult(key, record), AerospikeResult(key, record), AerospikeResult(key, record))
    }

    @Test
    fun testRxAsyncGetSequenceArrayKeyFailure() {
        val flowableGetSequenceArrayKey = rxClient.rxAsyncGetSequence(batchPolicy, keyArray).test()
        verify(mockClient).get(eq(batchPolicy), recordSequenceListener.capture(), eq(keyArray))
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onFailure(exception)
        flowableGetSequenceArrayKey.assertValues(AerospikeResult(key, record), AerospikeResult(key, record),AerospikeResult(key, record))
        flowableGetSequenceArrayKey.assertError(exception)
    }

    @Test
    fun testRxAsyncGetArrayBinSingleSuccess() {
        val singleGetArrayBin = rxClient.rxAsyncGet(batchPolicy, keyArray, binName).test()
        verify(mockClient).get(eq(batchPolicy), recordArrayListener.capture(), eq(keyArray), eq(binName))
        recordArrayListener.value.onSuccess(keyArray, recordArray)
        singleGetArrayBin.assertResult(AerospikeArrayResult(keyArray, recordArray))
    }

    @Test
    fun testRxAsyncGetArrayBinSingleFailure() {
        val singleGetArrayBin = rxClient.rxAsyncGet(batchPolicy, keyArray, binName).test()
        verify(mockClient).get(eq(batchPolicy), recordArrayListener.capture(), eq(keyArray), eq(binName))
        recordArrayListener.value.onFailure(exception)
        singleGetArrayBin.assertError(exception)
    }

    @Test
    fun testRxAsyncGetSequenceArrayKeyBinSuccess() {
        val flowableGetSequenceArrayKeyBin = rxClient.rxAsyncGetSequence(batchPolicy, keyArray, binName).test()
        verify(mockClient).get(eq(batchPolicy), recordSequenceListener.capture(), eq(keyArray), eq(binName))
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onSuccess()
        flowableGetSequenceArrayKeyBin.assertResult(AerospikeResult(key, record), AerospikeResult(key, record),AerospikeResult(key, record))
    }

    @Test
    fun testRxAsyncGetSequenceArrayKeyBinFailure() {
        val flowableGetSequenceArrayKeyBin = rxClient.rxAsyncGetSequence(batchPolicy, keyArray, binName).test()
        verify(mockClient).get(eq(batchPolicy), recordSequenceListener.capture(), eq(keyArray), eq(binName))
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onFailure(exception)
        flowableGetSequenceArrayKeyBin.assertValues(AerospikeResult(key, record), AerospikeResult(key, record),AerospikeResult(key, record))
        flowableGetSequenceArrayKeyBin.assertError(exception)
    }

    @Test
    fun testRxAsyncGetHeaderArraySuccess() {
        val singleGetHeaderArray = rxClient.rxAsyncGetHeader(batchPolicy, keyArray).test()
        verify(mockClient).getHeader(eq(batchPolicy), recordArrayListener.capture(), eq(keyArray))
        recordArrayListener.value.onSuccess(keyArray, recordArray)
        singleGetHeaderArray.assertResult(AerospikeArrayResult(keyArray, recordArray))
    }

    @Test
    fun testRxAsyncGetHeaderArrayFailure() {
        val singleGetHeaderArray = rxClient.rxAsyncGetHeader(batchPolicy, keyArray).test()
        verify(mockClient).getHeader(eq(batchPolicy), recordArrayListener.capture(), eq(keyArray))
        recordArrayListener.value.onFailure(exception)
        singleGetHeaderArray.assertError(exception)
    }

    @Test
    fun testRxAsyncGetHeaderSequenceArrayKeySuccess() {
        val flowableGetHeaderSequenceArrayKey = rxClient.rxAsyncGetHeaderSequence(batchPolicy, keyArray).test()
        verify(mockClient).getHeader(eq(batchPolicy), recordSequenceListener.capture(), eq(keyArray))
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onSuccess()
        flowableGetHeaderSequenceArrayKey.assertResult(AerospikeResult(key, record), AerospikeResult(key, record),AerospikeResult(key, record))
    }

    @Test
    fun testRxAsyncGetHeaderSequenceArrayKeyFailure() {
        val flowableGetHeaderSequenceArrayKey = rxClient.rxAsyncGetHeaderSequence(batchPolicy, keyArray).test()
        verify(mockClient).getHeader(eq(batchPolicy), recordSequenceListener.capture(), eq(keyArray))
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onFailure(exception)
        flowableGetHeaderSequenceArrayKey.assertValues(AerospikeResult(key, record), AerospikeResult(key, record),AerospikeResult(key, record))
        flowableGetHeaderSequenceArrayKey.assertError(exception)
    }

    @Test
    fun testRxAsyncOperateSuccess() {
        val singleOperate = rxClient.rxAsyncOperate(policy, key, operations[0]).test()
        verify(mockClient).operate(eq(policy), recordListener.capture(), eq(key), eq(operations[0]))
        recordListener.value.onSuccess(key, record)
        singleOperate.assertResult(AerospikeResult(key, record))
    }

    @Test
    fun testRxAsyncOperateFailure() {
        val singleOperate = rxClient.rxAsyncOperate(policy, key, operations[0]).test()
        verify(mockClient).operate(eq(policy), recordListener.capture(), eq(key), eq(operations[0]))
        recordListener.value.onFailure(exception)
        singleOperate.assertError(exception)
    }

    @Test
    fun testRxAsyncScanAllSuccess() {
        val flowableScanAll = rxClient.rxAsyncScanAll(scanPolicy, namespace, setName, binName).test()
        verify(mockClient).scanAll(eq(scanPolicy), recordSequenceListener.capture(), eq(namespace), eq(setName), eq(binName))
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onSuccess()
        flowableScanAll.assertResult(AerospikeResult(key, record), AerospikeResult(key, record),AerospikeResult(key, record))
    }

    @Test
    fun testRxAsyncScanAllFailure() {
        val flowableScanAll = rxClient.rxAsyncScanAll(scanPolicy, namespace, setName, binName).test()
        verify(mockClient).scanAll(eq(scanPolicy), recordSequenceListener.capture(), eq(namespace), eq(setName), eq(binName))
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onFailure(exception)
        flowableScanAll.assertValues(AerospikeResult(key, record), AerospikeResult(key, record),AerospikeResult(key, record))
        flowableScanAll.assertError(exception)
    }

    @Test
    fun testRxAsyncExecuteSuccess() {
        val singleExecute = rxClient.rxAsyncExecute(policy, key, packageName, functionName, value).test()
        verify(mockClient).execute(eq(policy), executeListenerCaptor.capture(), eq(key), eq(packageName), eq(functionName), eq(value))
        executeListenerCaptor.value.onSuccess(key, value)
        singleExecute.assertResult(AerospikeResult(key, value))
    }

    @Test
    fun testRxAsyncExecuteFailure() {
        val singleExecute = rxClient.rxAsyncExecute(policy, key, packageName, functionName, value).test()
        verify(mockClient).execute(eq(policy), executeListenerCaptor.capture(), eq(key), eq(packageName), eq(functionName), eq(value))
        executeListenerCaptor.value.onFailure(exception)
        singleExecute.assertError(exception)
    }

    @Test
    fun testRxAsyncQuerySuccess() {
        val flowableQuery = rxClient.rxAsyncQuery(queryPolicy, statement).test()
        verify(mockClient).query(eq(queryPolicy), recordSequenceListener.capture(), eq(statement))
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onSuccess()
        flowableQuery.assertResult(AerospikeResult(key, record), AerospikeResult(key, record),AerospikeResult(key, record))
    }

    @Test
    fun testRxAsyncQueryFailure() {
        val flowableQuery = rxClient.rxAsyncQuery(queryPolicy, statement).test()
        verify(mockClient).query(eq(queryPolicy), recordSequenceListener.capture(), eq(statement))
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onFailure(exception)
        flowableQuery.assertValues(AerospikeResult(key, record), AerospikeResult(key, record),AerospikeResult(key, record))
        flowableQuery.assertError(exception)
    }

    @Test
    fun testRxRegisterNoClassLoaderSuccess() {
        doNothing().`when`(registerTask).waitTillComplete(1000)
        `when`(mockClient.register(eq(policy), eq(clientPath), eq(serverPath), eq(language)))
                .thenReturn(registerTask)
        val registerCompletable = rxClient.rxRegister(policy, clientPath, serverPath, language).test()
        verify(mockClient).register(eq(policy), eq(clientPath), eq(serverPath), eq(language))
        registerCompletable.assertComplete()
    }

    @Test
    fun testRxRegisterNoClassLoaderFailure() {
        doThrow(exception).`when`(registerTask).waitTillComplete(1000)
        `when`(mockClient.register(eq(policy), eq(clientPath), eq(serverPath), eq(language)))
                .thenReturn(registerTask)
        val registerCompletable = rxClient.rxRegister(policy, clientPath, serverPath, language).test()
        verify(mockClient).register(eq(policy), eq(clientPath), eq(serverPath), eq(language))
        registerCompletable.assertError(exception)
    }

    @Test
    fun testRxRegisterClassLoaderSuccess() {
        doNothing().`when`(registerTask).waitTillComplete(1000)
        `when`(mockClient.register(eq(policy), eq(classLoader), eq(clientPath), eq(serverPath), eq(language)))
                .thenReturn(registerTask)
        val registerCompletable = rxClient.rxRegister(policy, classLoader, clientPath, serverPath, language).test()
        verify(mockClient).register(eq(policy), eq(classLoader), eq(clientPath), eq(serverPath), eq(language))
        registerCompletable.assertComplete()
    }

    @Test
    fun testRxRegisterClassLoaderFailure() {
        doThrow(exception).`when`(registerTask).waitTillComplete(1000)
        `when`(mockClient.register(eq(policy), eq(classLoader), eq(clientPath), eq(serverPath), eq(language)))
                .thenReturn(registerTask)
        val registerCompletable = rxClient.rxRegister(policy, classLoader, clientPath, serverPath, language).test()
        verify(mockClient).register(eq(policy), eq(classLoader), eq(clientPath), eq(serverPath), eq(language))
        registerCompletable.assertError(exception)
    }

    @Test
    fun testRxRegisterUdfSuccess() {
        doNothing().`when`(registerTask).waitTillComplete(1000)
        `when`(mockClient.registerUdfString(eq(policy), eq(binName), eq(serverPath), eq(language)))
                .thenReturn(registerTask)
        val registerCompletable = rxClient.rxRegisterUdfString(policy, binName, serverPath, language).test()
        verify(mockClient).registerUdfString(eq(policy), eq(binName), eq(serverPath), eq(language))
        registerCompletable.assertComplete()
    }

    @Test
    fun testRxRegisterUdfFailure() {
        doThrow(exception).`when`(registerTask).waitTillComplete(1000)
        `when`(mockClient.registerUdfString(eq(policy), eq(binName), eq(serverPath), eq(language)))
                .thenReturn(registerTask)
        val registerCompletable = rxClient.rxRegisterUdfString(policy, binName, serverPath, language).test()
        verify(mockClient).registerUdfString(eq(policy), eq(binName), eq(serverPath), eq(language))
        registerCompletable.assertError(exception)
    }

    @Test
    fun testRxExecuteSuccess() {
        doNothing().`when`(executeTask).waitTillComplete(1000)
        `when`(mockClient.execute(eq(policy), eq(statement), eq(packageName), eq(functionName), eq(value)))
                .thenReturn(executeTask)
        val executeCompletable = rxClient.rxExecute(policy, statement, packageName, functionName, 1000, value).test()
        verify(mockClient).execute(eq(policy), eq(statement), eq(packageName), eq(functionName), eq(value))
        executeCompletable.assertComplete()
    }

    @Test
    fun testRxExecuteFailure() {
        doThrow(exception).`when`(executeTask).waitTillComplete(1000)
        `when`(mockClient.execute(eq(policy), eq(statement), eq(packageName), eq(functionName), eq(value)))
                .thenReturn(executeTask)
        val executeCompletable = rxClient.rxExecute(policy, statement, packageName, functionName, 1000, value).test()
        verify(mockClient).execute(eq(policy), eq(statement), eq(packageName), eq(functionName), eq(value))
        executeCompletable.assertError(exception)
    }

    @Test
    fun testRxCreateIndexSuccess() {
        doNothing().`when`(indexTask).waitTillComplete(1000)
        `when`(mockClient.createIndex(eq(policy), eq(namespace), eq(setName), eq(indexName), eq(binName), eq(indexType)))
                .thenReturn(indexTask)
        val createIndexCompletable = rxClient.rxCreateIndex(policy, namespace, setName, indexName, binName, indexType).test()
        verify(mockClient).createIndex(eq(policy), eq(namespace), eq(setName), eq(indexName), eq(binName), eq(indexType))
        createIndexCompletable.assertComplete()
    }

    @Test
    fun testRxCreateIndexFailure() {
        doThrow(exception).`when`(indexTask).waitTillComplete(1000)
        `when`(mockClient.createIndex(eq(policy), eq(namespace), eq(setName), eq(indexName), eq(binName), eq(indexType)))
                .thenReturn(indexTask)
        val createIndexCompletable = rxClient.rxCreateIndex(policy, namespace, setName, indexName, binName, indexType).test()
        verify(mockClient).createIndex(eq(policy), eq(namespace), eq(setName), eq(indexName), eq(binName), eq(indexType))
        createIndexCompletable.assertError(exception)
    }

    @Test
    fun testRxCreateIndexWithCollectionTypeSuccess() {
        doNothing().`when`(indexTask).waitTillComplete(1000)
        `when`(mockClient.createIndex(eq(policy), eq(namespace), eq(setName), eq(indexName), eq(binName), eq(indexType), eq(indexCollectionType)))
                .thenReturn(indexTask)
        val createIndexCompletable = rxClient.rxCreateIndex(policy, namespace, setName, indexName, binName, indexType, indexCollectionType).test()
        verify(mockClient).createIndex(eq(policy), eq(namespace), eq(setName), eq(indexName), eq(binName), eq(indexType), eq(indexCollectionType))
        createIndexCompletable.assertComplete()
    }

    @Test
    fun testRxCreateIndexWithCollectionTypeFailure() {
        doThrow(exception).`when`(indexTask).waitTillComplete(1000)
        `when`(mockClient.createIndex(eq(policy), eq(namespace), eq(setName), eq(indexName), eq(binName), eq(indexType), eq(indexCollectionType)))
                .thenReturn(indexTask)
        val createIndexCompletable = rxClient.rxCreateIndex(policy, namespace, setName, indexName, binName, indexType, indexCollectionType).test()
        verify(mockClient).createIndex(eq(policy), eq(namespace), eq(setName), eq(indexName), eq(binName), eq(indexType), eq(indexCollectionType))
        createIndexCompletable.assertError(exception)
    }
}
