package io.odpf.depot.redis.parsers;


import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.config.enums.SinkConnectorSchemaDataType;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.message.*;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.InvalidConfigurationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Convert kafka messages to RedisDataEntry.
 */
@AllArgsConstructor
public abstract class RedisParser {
    private OdpfMessageParser odpfMessageParser;
    private OdpfSinkConfig sinkConfig;
    public abstract List<RedisDataEntry> parse(OdpfMessage message) throws IOException;

    public List<RedisDataEntry> parse(List<OdpfMessage> messages, OdpfSinkResponse odpfSinkResponse) {
        List<RedisDataEntry> list = new ArrayList<>();

        for(int i = 0; i < messages.size(); i++) {
            try {
                List<RedisDataEntry> p = parse(messages.get(i));
                list.addAll(p);
            }
            catch (InvalidConfigurationException e) {
                // key template does not have a comma
                odpfSinkResponse.addErrors(i, new ErrorInfo(e, ErrorType.DEFAULT_ERROR));
            }
            catch (IllegalArgumentException e) {
                // empty key template found
                odpfSinkResponse.addErrors(i, new ErrorInfo(e, ErrorType.DEFAULT_ERROR));
            }
            catch (IOException e) {
                // when deserializing invalid byte array found
                odpfSinkResponse.addErrors(i, new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
            }
            catch (Exception e) {
                // generic handler
                odpfSinkResponse.addErrors(i, new ErrorInfo(e, ErrorType.DEFAULT_ERROR));
            }
        }

        return list;
    }

    /**
     * Parse esb message to protobuf.
     *
     * @param message parsed message
     * @return Parsed Proto object
     */
    ParsedOdpfMessage parseEsbMessage(OdpfMessage message) throws IOException {
        String schemaClass;
        if (sinkConfig.getSinkConnectorSchemaMessageMode().equals(SinkConnectorSchemaMessageMode.LOG_KEY)) {
            schemaClass = sinkConfig.getSinkConnectorSchemaKeyClass();
        } else {
            schemaClass = sinkConfig.getSinkConnectorSchemaMessageClass();
        }
        //TODO : depot config set
        schemaClass = "io.odpf.firehose.consumer.TestMessage";

        ParsedOdpfMessage parsedOdpfMessage = odpfMessageParser.parse(message, SinkConnectorSchemaMessageMode.LOG_MESSAGE, schemaClass);
        return parsedOdpfMessage;
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
        if(sinkConfig.getSinkConnectorSchemaDataType().equals(SinkConnectorSchemaDataType.PROTOBUF)) {
            DynamicMessage dynamicMessage = (DynamicMessage) parsedMessage.getRaw();
            Descriptors.FieldDescriptor fieldDescriptor = dynamicMessage.getDescriptorForType().findFieldByNumber(fieldNumberInt);
            if (fieldDescriptor == null) {
                throw new IllegalArgumentException(String.format("Descriptor not found for index: %s", fieldNumber));
            }
            return dynamicMessage.getField(fieldDescriptor);
        } else {
            //TODO: logic if JSON
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
        if (sinkConfig.getSinkConnectorSchemaMessageMode().equals(SinkConnectorSchemaMessageMode.LOG_KEY)) {
            return (byte[]) message.getLogKey();
        } else {
            return (byte[]) message.getLogMessage();
        }
    }
}
