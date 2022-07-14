//package io.odpf.depot.redis;
//
//import io.odpf.depot.message.OdpfMessage;
//import io.odpf.firehose.message.Message;
//import io.odpf.firehose.metrics.FirehoseInstrumentation;
//import io.odpf.depot.redis.client.RedisClient;
//import io.odpf.depot.redis.exception.NoResponseException;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.InOrder;
//import org.mockito.Mock;
//import org.mockito.junit.MockitoJUnitRunner;
//
//import java.time.Instant;
//import java.util.ArrayList;
//import static org.mockito.Mockito.*;
//
//@RunWith(MockitoJUnitRunner.class)
//public class RedisSinkTest {
//    @Mock
//    private RedisClient redisClient;
//    @Mock
//    private FirehoseInstrumentation firehoseInstrumentation;
//    private RedisSink redis;
//
//    @Before
//    public void setup() {
//        when(firehoseInstrumentation.startExecution()).thenReturn(Instant.now());
//        redis = new RedisSink(firehoseInstrumentation, "redis", redisClient);
//    }
//
//    @Test
//    public void shouldInvokePushToSinkOnTheClient() {
//        ArrayList<OdpfMessage> messages = new ArrayList<>();
//        redis.pushToSink(messages);
//        //verify(redisClient).prepare(messages);
//    }
//
//    @Test
//    public void shouldInvokePrepareOnTheClient() {
//        ArrayList<Message> messages = new ArrayList<>();
//
//        redis.prepare(messages);
//
//        verify(redisClient).prepare(messages);
//    }
//
//    @Test
//    public void shouldInvokeCloseOnTheClient() {
//        redis.close();
//
//        verify(redisClient).close();
//    }
//
//    @Test
//    public void shouldLogWhenClosingConnection() {
//        redis.close();
//
//        verify(firehoseInstrumentation, times(1)).logInfo("Redis connection closing");
//    }
//
//    @Test
//    public void sendsMetricsForSuccessMessages() {
//        ArrayList<Message> messages = new ArrayList<>();
//
//        redis.pushMessage(messages);
//
//        verify(firehoseInstrumentation, times(1)).capturePreExecutionLatencies(messages);
//        verify(firehoseInstrumentation, times(1)).startExecution();
//        verify(firehoseInstrumentation, times(1)).logInfo("Preparing {} messages", messages.size());
//        verify(firehoseInstrumentation, times(1)).captureSinkExecutionTelemetry(any(), any());
//        InOrder inOrder = inOrder(firehoseInstrumentation);
//        inOrder.verify(firehoseInstrumentation).logInfo("Preparing {} messages", messages.size());
//        inOrder.verify(firehoseInstrumentation).capturePreExecutionLatencies(messages);
//        inOrder.verify(firehoseInstrumentation).startExecution();
//        inOrder.verify(firehoseInstrumentation).captureSinkExecutionTelemetry(any(), any());
//    }
//
//    @Test
//    public void sendsMetricsForFailedMessages() {
//        when(redisClient.execute()).thenThrow(new NoResponseException());
//        ArrayList<Message> messages = new ArrayList<>();
//
//        redis.pushMessage(messages);
//
//        verify(firehoseInstrumentation, times(1)).capturePreExecutionLatencies(messages);
//        verify(firehoseInstrumentation, times(1)).startExecution();
//        verify(firehoseInstrumentation, times(1)).logInfo("Preparing {} messages", messages.size());
//        verify(firehoseInstrumentation, times(1)).captureSinkExecutionTelemetry(any(), any());
//        InOrder inOrder = inOrder(firehoseInstrumentation);
//        inOrder.verify(firehoseInstrumentation).logInfo("Preparing {} messages", messages.size());
//        inOrder.verify(firehoseInstrumentation).capturePreExecutionLatencies(messages);
//        inOrder.verify(firehoseInstrumentation).startExecution();
//        inOrder.verify(firehoseInstrumentation).captureSinkExecutionTelemetry(any(), any());
//    }
//
//
//}
