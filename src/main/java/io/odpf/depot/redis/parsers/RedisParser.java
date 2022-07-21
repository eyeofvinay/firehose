package io.odpf.depot.redis.parsers;


import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.firehose.config.AppConfig;
import io.odpf.firehose.message.Message;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.stencil.Parser;
import lombok.AllArgsConstructor;
import org.aeonbits.owner.ConfigFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.InvalidConfigurationException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Convert kafka messages to RedisDataEntry.
 */
@AllArgsConstructor
public abstract class RedisParser {
    private OdpfMessageParser odpfMessageParser;
    private RedisSinkConfig redisSinkConfig;
    public abstract List<RedisDataEntry> parse(OdpfMessage message);

    public List<RedisDataEntry> parse(List<OdpfMessage> messages) {
        return messages
                .stream()
                .map(this::parse)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    /**
     * Parse esb message to protobuf.
     *
     * @param message parsed message
     * @return Parsed Proto object
     */
    ParsedOdpfMessage parseEsbMessage(OdpfMessage message) {
        String schemaClass = ConfigFactory.create(AppConfig.class, System.getenv()).getInputSchemaProtoClass();
        try {
            ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass);
            return parsedOdpfMessage;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Parse template string.
     *
     * @param data     the data
     * @param template the template
     * @return parsed template
     */
    String parseTemplate(ParsedOdpfMessage data, String template) {
        if (StringUtils.isEmpty(template)) {
            throw new IllegalArgumentException("Template '" + template + "' is invalid");
        }
        String[] templateStrings = template.split(",");
        if (templateStrings.length == 0) {
            throw new InvalidConfigurationException("Empty key configuration: '" + template + "'");
        }
        templateStrings = Arrays
                .stream(templateStrings)
                .map(String::trim)
                .toArray(String[]::new);
        String templatePattern = templateStrings[0];
        String templateVariables = StringUtils.join(Arrays.copyOfRange(templateStrings, 1, templateStrings.length), ",");
        String renderedTemplate = renderStringTemplate(data, templatePattern, templateVariables);
        return StringUtils.isEmpty(templateVariables)
                ? templatePattern
                : renderedTemplate;
    }

    private String renderStringTemplate(ParsedOdpfMessage parsedMessage, String pattern, String patternVariables) {
        if (StringUtils.isEmpty(patternVariables)) {
            return pattern;
        }
        List<String> patternVariableFieldNumbers = Arrays.asList(patternVariables.split(","));
        Object[] patternVariableData = patternVariableFieldNumbers
                .stream()
                .map(fieldNumber -> getDataByFieldNumber(parsedMessage, fieldNumber))
                .toArray();
        return String.format(pattern, patternVariableData);
    }

    /**
     * Gets data by field number.
     *
     * @param parsedMessage the parsed message
     * @param fieldNumber   the field number
     * @return Data object
     */
    Object getDataByFieldNumber(ParsedOdpfMessage parsedMessage, String fieldNumber) {
        int fieldNumberInt;
        try {
            fieldNumberInt = Integer.parseInt(fieldNumber);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Index");
        }
        if(redisSinkConfig.getSinkRedisInputMessageType().equals("PROTO"

        )) {
            DynamicMessage dynamicMessage = (DynamicMessage) parsedMessage.getRaw();
            Descriptors.FieldDescriptor fieldDescriptor = dynamicMessage.getDescriptorForType().findFieldByNumber(fieldNumberInt);
            if (fieldDescriptor == null) {
                throw new IllegalArgumentException(String.format("Descriptor not found for index: %s", fieldNumber));
            }
            return dynamicMessage.getField(fieldDescriptor);
        } else {
            return new Object();
        }
    }

    /**
     * Get payload bytes.
     *
     * @param message the message
     * @return binary payload
     */
    byte[] getPayload(OdpfMessage message) {
        if (redisSinkConfig.getKafkaRecordParserMode().equals("key")) {
            return (byte[]) message.getLogKey();
        } else {
            return (byte[]) message.getLogMessage();
        }
    }
}
