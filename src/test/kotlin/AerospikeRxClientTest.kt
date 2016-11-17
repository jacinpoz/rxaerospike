
import com.aerospike.client.AerospikeException
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.async.AsyncClient
import com.aerospike.client.listener.DeleteListener
import com.aerospike.client.listener.ExistsListener
import com.aerospike.client.listener.WriteListener
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
    val key = Key("test", "test", "test")
    val bin = Bin("TestBin", 2.0)
    val exception = AerospikeException()

    var writeListenerCaptor = ArgumentCaptor.forClass(WriteListener::class.java)
    var deleteListenerCaptor = ArgumentCaptor.forClass(DeleteListener::class.java)
    var existsListenerCaptor = ArgumentCaptor.forClass(ExistsListener::class.java)
    var mockClient = mock(AsyncClient::class.java)
    var rxClient = AerospikeRxClient(mockClient)

    @Before
    fun initialiseMockVariables(){
        writeListenerCaptor = ArgumentCaptor.forClass(WriteListener::class.java)
        deleteListenerCaptor = ArgumentCaptor.forClass(DeleteListener::class.java)
        mockClient = mock(AsyncClient::class.java)
        rxClient = AerospikeRxClient(mockClient)
    }

    @Test
    fun testRxPutSuccess(){
        val singlePut = rxClient.rxAsyncPut(policy, key, bin).test()
        verify(mockClient).put(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onSuccess(key)
        singlePut.assertResult(key)
    }

    @Test
    fun testRxPutFailure(){
        val singlePut = rxClient.rxAsyncPut(policy, key, bin).test()
        verify(mockClient).put(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onFailure(exception)
        singlePut.assertError(exception)
    }

    @Test
    fun testRxAppendSuccess(){
        val singleAppend = rxClient.rxAsyncAppend(policy, key, bin).test()
        verify(mockClient).append(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onSuccess(key)
        singleAppend.assertResult(key)
    }

    @Test
    fun testRxAppendFailure(){
        val singleAppend = rxClient.rxAsyncAppend(policy, key, bin).test()
        verify(mockClient).append(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onFailure(exception)
        singleAppend.assertError(exception)
    }


    @Test
    fun testRxPrependSuccess(){
        val singlePrepend = rxClient.rxAsyncPrepend(policy, key, bin).test()
        verify(mockClient).prepend(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onSuccess(key)
        singlePrepend.assertResult(key)
    }

    @Test
    fun testRxPrependFailure(){
        val singlePrepend = rxClient.rxAsyncPrepend(policy, key, bin).test()
        verify(mockClient).prepend(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onFailure(exception)
        singlePrepend.assertError(exception)
    }

    @Test
    fun testRxAddSuccess(){
        val singleAdd = rxClient.rxAsyncAdd(policy, key, bin).test()
        verify(mockClient).add(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onSuccess(key)
        singleAdd.assertResult(key)
    }

    @Test
    fun testRxAddFailure(){
        val singleAdd = rxClient.rxAsyncAdd(policy, key, bin).test()
        verify(mockClient).add(eq(policy), writeListenerCaptor.capture(), eq(key), eq(bin))
        writeListenerCaptor.value.onFailure(exception)
        singleAdd.assertError(exception)
    }

    @Test
    fun testRxDeleteSuccess(){
        val singleDelete = rxClient.rxAsyncDelete(policy, key).test()
        verify(mockClient).delete(eq(policy), deleteListenerCaptor.capture(), eq(key))
        deleteListenerCaptor.value.onSuccess(key, true)
        singleDelete.assertResult(key to true)
    }

    @Test
    fun testRxDeleteFailure(){
        val singleDelete = rxClient.rxAsyncDelete(policy, key).test()
        verify(mockClient).delete(eq(policy), deleteListenerCaptor.capture(), eq(key))
        deleteListenerCaptor.value.onFailure(exception)
        singleDelete.assertError(exception)
    }

    @Test
    fun testRxTouchSuccess(){
        val singleTouch = rxClient.rxAsyncTouch(policy, key).test()
        verify(mockClient).touch(eq(policy), writeListenerCaptor.capture(), eq(key))
        writeListenerCaptor.value.onSuccess(key)
        singleTouch.assertResult(key)
    }

    @Test
    fun testRxTouchFailure(){
        val singleTouch = rxClient.rxAsyncTouch(policy, key).test()
        verify(mockClient).touch(eq(policy), writeListenerCaptor.capture(), eq(key))
        writeListenerCaptor.value.onFailure(exception)
        singleTouch.assertError(exception)
    }

    @Test
    fun testRxExistsSuccess(){
        val singleExists = rxClient.rxAsyncExists(policy, key).test()
        verify(mockClient).exists(eq(policy), existsListenerCaptor.capture(), eq(key))
        existsListenerCaptor.value.onSuccess(key, true)
        singleExists.assertResult(key to true)
    }

    @Test
    fun testRxExistsFailure(){
        val singleExists = rxClient.rxAsyncExists(policy, key).test()
        verify(mockClient).exists(eq(policy), existsListenerCaptor.capture(), eq(key))
        existsListenerCaptor.value.onFailure(exception)
        singleExists.assertError(exception)
    }
}