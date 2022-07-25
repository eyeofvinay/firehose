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
//
//import static io.odpf.firehose.message.FirehoseMessageUtils.convertToOdpfMessage;
//import static org.mockito.Mockito.*;
//import org.mockito.junit.MockitoJUnitRunner;
//
//import java.io.IOException;
//import java.time.Instant;
//import java.util.ArrayList;
//
//import static io.odpf.depot.message.DepotMessageUtils.convertOdpfMessageToMessage;
//import static org.mockito.Mockito.verify;
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
//        InOrder inOrder = inOrder(redisClient);
//        inOrder.verify(redisClient).prepare(convertOdpfMessageToMessage(messages));
//        inOrder.verify(redisClient).execute();
//    }
//
//
//    @Test
//    public void shouldInvokeCloseOnTheClient() throws IOException {
//        redis.close();
//
//        verify(redisClient).close();
//    }
//
//    @Test
//    public void shouldLogWhenClosingConnection() throws IOException {
//        redis.close();
//
//        verify(firehoseInstrumentation, times(1)).logInfo("Redis connection closing");
//    }
//
//    @Test
//    public void sendsMetricsForSuccessMessages() {
//        ArrayList<Message> messages = new ArrayList<>();
//
//        redis.pushToSink(convertToOdpfMessage(messages));
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
//        redis.pushToSink(convertToOdpfMessage(messages));
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
