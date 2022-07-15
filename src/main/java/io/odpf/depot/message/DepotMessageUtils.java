package io.odpf.depot.message;

import io.odpf.firehose.message.Message;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.common.header.Headers;

public class DepotMessageUtils {

    public static List<Message> convertOdpfMessageToMessage(List<OdpfMessage> messages) {
        return messages.stream().map(message ->
                        new Message(
                                (byte[]) message.getLogKey(),
                                (byte[]) message.getLogMessage(),
                                (String) message.getMetadata().get("message_topic"),
                                (Integer) message.getMetadata().get("message_partition"),
                                (Long) message.getMetadata().get("message_offset"),
                                (Headers) message.getMetadata().get("message_headers"),
                                (Long) message.getMetadata().get("message_timestamp"),
                                (Long) message.getMetadata().get("load_time")
                        ))
                .collect(Collectors.toList());
    }
}
