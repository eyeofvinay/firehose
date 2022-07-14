package io.odpf.depot.redis;

import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.depot.redis.client.RedisClient;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class RedisSink implements OdpfSink {
    private FirehoseInstrumentation firehoseInstrumentation;
    private String redis;
    private RedisClient client;

    public RedisSink(FirehoseInstrumentation firehoseInstrumentation, String redis, RedisClient client) {
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.redis = redis;
        this.client = client;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public OdpfSinkResponse pushToSink(List<OdpfMessage> messageList) {
        OdpfSinkResponse odpfSinkResponse = new OdpfSinkResponse();
        client.prepare(convertOdpfMessageToMessage(messageList));
        List<Message> messages = client.execute();
//        for (int i = 0; i < messages.size(); i++) {
////            if() {
////                odpfSinkResponse.addErrors();
////            }
//        }
        return odpfSinkResponse;
    }

    public static List<Message> convertOdpfMessageToMessage(List<OdpfMessage> messages) {
        return messages.stream().map(message ->
                        new Message(
                                (byte[]) message.getLogKey(),
                                (byte[]) message.getLogMessage(),
                                (String) message.getMetadata().get("message_topic"),
                                (Integer) message.getMetadata().get("message_partition"),
                                (Long) message.getMetadata().get("message_offset")
                                ))
                .collect(Collectors.toList());
    }
}
