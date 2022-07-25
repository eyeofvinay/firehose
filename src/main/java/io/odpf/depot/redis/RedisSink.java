package io.odpf.depot.redis;

import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.firehose.exception.ConfigurationException;
import io.odpf.firehose.exception.DefaultException;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.exception.SinkException;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.depot.redis.client.RedisClient;
import io.odpf.firehose.metrics.Metrics;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static io.odpf.depot.message.DepotMessageUtils.convertOdpfMessageToMessage;
import static io.odpf.firehose.metrics.Metrics.SINK_MESSAGES_TOTAL;

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
        firehoseInstrumentation.logInfo("Redis connection closing");

        client.close();
    }

    @Override
    public OdpfSinkResponse pushToSink(List<OdpfMessage> messages) {
        OdpfSinkResponse odpfSinkResponse = new OdpfSinkResponse();
        client.prepare(messages, odpfSinkResponse);
        client.execute();
        return odpfSinkResponse;
    }
}
