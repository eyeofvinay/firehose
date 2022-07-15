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
    public OdpfSinkResponse pushToSink(List<OdpfMessage> odpfMessages) {
        List<Message> messages = convertOdpfMessageToMessage(odpfMessages);
        List<Message> failedMessages = messages;
        Instant executionStartTime = null;
        try {
            firehoseInstrumentation.logInfo("Preparing {} messages", odpfMessages.size());
            firehoseInstrumentation.captureMessageBatchSize(odpfMessages.size());
            firehoseInstrumentation.captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.TOTAL, odpfMessages.size());
            client.prepare(messages);
            firehoseInstrumentation.capturePreExecutionLatencies(messages);
            executionStartTime = firehoseInstrumentation.startExecution();
            failedMessages = client.execute();
            firehoseInstrumentation.logInfo("Pushed {} messages", messages.size() - failedMessages.size());
        } catch (DeserializerException | ConfigurationException | NullPointerException | SinkException e) {
            throw e;
        } catch (Exception e) {
            if (!messages.isEmpty()) {
                firehoseInstrumentation.logWarn("Failed to push {} messages to sink", messages.size());
            }
            //firehoseInstrumentation.captureNonFatalError(e, "caught {} {}", e.getClass(), e.getMessage());
            failedMessages = messages;
        } finally {
            // Process success,failure and error metrics
            if (executionStartTime != null) {
                firehoseInstrumentation.captureSinkExecutionTelemetry("redis", messages.size());
            }
            firehoseInstrumentation.captureMessageMetrics(Metrics.SINK_MESSAGES_TOTAL, Metrics.MessageType.SUCCESS, messages.size() - failedMessages.size());
            firehoseInstrumentation.captureGlobalMessageMetrics(Metrics.MessageScope.SINK, messages.size() - failedMessages.size());
            processFailedMessages(failedMessages);
        }

        return failedMessagesToOdpfSinkResponse(failedMessages);
    }

    private OdpfSinkResponse failedMessagesToOdpfSinkResponse(List<Message> failedMessages) {
        OdpfSinkResponse odpfSinkResponse = new OdpfSinkResponse();
        //TODO: logic
        for(int i = 0; i < failedMessages.size(); i++) {
            odpfSinkResponse.addErrors(i, new ErrorInfo(new DefaultException("DEFAULT"), ErrorType.DEFAULT_ERROR));
        }
        return odpfSinkResponse;
    }
    private void processFailedMessages(List<Message> failedMessages) {
        if (failedMessages.size() > 0) {
            firehoseInstrumentation.logError("Failed to Push {} messages to sink ", failedMessages.size());
            failedMessages.forEach(m -> {
                m.setDefaultErrorIfNotPresent();
                firehoseInstrumentation.captureMessageMetrics(SINK_MESSAGES_TOTAL, Metrics.MessageType.FAILURE, m.getErrorInfo().getErrorType(), 1);
                firehoseInstrumentation.captureErrorMetrics(m.getErrorInfo().getErrorType());
                firehoseInstrumentation.logError("Failed to Push message. Error: {},Topic: {}, Partition: {},Offset: {}",
                        m.getErrorInfo().getErrorType(),
                        m.getTopic(),
                        m.getPartition(),
                        m.getOffset());
            });
        }
    }

}
