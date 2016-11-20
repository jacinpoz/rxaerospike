
import com.aerospike.client.AerospikeException
import com.aerospike.client.BatchRead
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.Record
import com.aerospike.client.async.AsyncClient
import com.aerospike.client.listener.BatchListListener
import com.aerospike.client.listener.BatchSequenceListener
import com.aerospike.client.listener.DeleteListener
import com.aerospike.client.listener.ExistsArrayListener
import com.aerospike.client.listener.ExistsListener
import com.aerospike.client.listener.ExistsSequenceListener
import com.aerospike.client.listener.RecordArrayListener
import com.aerospike.client.listener.RecordListener
import com.aerospike.client.listener.RecordSequenceListener
import com.aerospike.client.listener.WriteListener
import com.aerospike.client.policy.BatchPolicy
import com.aerospike.client.policy.WritePolicy
import com.github.ganet.rx.aerospike.AerospikeRxClient
import org.junit.Before
import org.junit.Test
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify


/**
 * Created by JoseIgnacio on 17/11/2016.
 */
class AerospikeRxClientTest {
    val policy = WritePolicy()
    val batchPolicy = BatchPolicy()
    val key = Key("test", "test", "test")
    val keyArray = arrayOf(Key("test", "test", "test"))
    val booleanArray = booleanArrayOf(true)
    val binName = "TestBin"
    val bin = Bin(binName, 2.0)
    val listOfBatchRead = listOf(BatchRead(key, true), BatchRead(key, false), BatchRead(key, true))
    val record = Record(emptyMap(), 1, 1)
    val recordArray : Array<Record?> = arrayOf(record)
    val exception = AerospikeException()

    var writeListenerCaptor = ArgumentCaptor.forClass(WriteListener::class.java)
    var deleteListenerCaptor = ArgumentCaptor.forClass(DeleteListener::class.java)

    var existsListenerCaptor = ArgumentCaptor.forClass(ExistsListener::class.java)
    var existsArrayListenerCaptor = ArgumentCaptor.forClass(ExistsArrayListener::class.java)
    var existsSequenceListenerCaptor = ArgumentCaptor.forClass(ExistsSequenceListener::class.java)

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
        singleDelete.assertResult(key to true)
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
        singleExists.assertResult(key to true)
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
        singleExists.assertResult(keyArray to booleanArray)
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
        flowableExists.assertResult(key to true, key to false, key to true)
    }

    @Test
    fun testRxAsyncExistsSequenceFailure() {
        val flowableExists = rxClient.rxAsyncExistsSequence(batchPolicy, keyArray).test()
        verify(mockClient).exists(eq(batchPolicy), existsSequenceListenerCaptor.capture(), eq(keyArray))
        existsSequenceListenerCaptor.value.onExists(key, true)
        existsSequenceListenerCaptor.value.onExists(key, false)
        existsSequenceListenerCaptor.value.onExists(key, true)
        existsSequenceListenerCaptor.value.onFailure(exception)
        flowableExists.assertValues(key to true, key to false, key to true)
        flowableExists.assertError(exception)
    }

    @Test
    fun testRxAsyncGetSuccess() {
        val singleGet = rxClient.rxAsyncGet(policy, key).test()
        verify(mockClient).get(eq(policy), recordListener.capture(), eq(key))
        recordListener.value.onSuccess(key, record)
        singleGet.assertResult(key to record)
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
        singleGetBins.assertResult(key to record)
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
        singleGetHeader.assertResult(key to record)
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
        singleGetBatchArrayKeyRead.assertResult(keyArray to recordArray)
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
        val flowableGetSequenceArraykey = rxClient.rxAsyncGetSequence(batchPolicy, keyArray).test()
        verify(mockClient).get(eq(batchPolicy), recordSequenceListener.capture(), eq(keyArray))
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onSuccess()
        flowableGetSequenceArraykey.assertResult(key to record, key to record, key to record)
    }

    @Test
    fun testRxAsyncGetSequenceArrayKeyFailure() {
        val flowableGetSequenceArraykey = rxClient.rxAsyncGetSequence(batchPolicy, keyArray).test()
        verify(mockClient).get(eq(batchPolicy), recordSequenceListener.capture(), eq(keyArray))
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onRecord(key, record)
        recordSequenceListener.value.onFailure(exception)
        flowableGetSequenceArraykey.assertValues(key to record, key to record, key to record)
        flowableGetSequenceArraykey.assertError(exception)
    }
}
