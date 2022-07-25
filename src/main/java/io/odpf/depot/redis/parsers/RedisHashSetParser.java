package io.odpf.depot.redis.parsers;

import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.redis.dataentry.RedisHashSetFieldEntry;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import com.google.protobuf.DynamicMessage;
import io.odpf.stencil.Parser;
import org.aeonbits.owner.ConfigFactory;
import org.apache.hadoop.fs.Stat;
import org.apache.logging.log4j.core.pattern.AbstractStyleNameConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Redis hash set parser.
 */
public class RedisHashSetParser extends RedisParser {
    private RedisSinkConfig redisSinkConfig;
    private ProtoToFieldMapper protoToFieldMapper;
    private StatsDReporter statsDReporter;

    /**
     * Instantiates a new Redis hash set parser.
     *  @param protoToFieldMapper the proto to field mapper
     * @param redisSinkConfig    the redis sink config
     * @param statsDReporter     the statsd reporter
     */
    public RedisHashSetParser(ProtoToFieldMapper protoToFieldMapper, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        super(OdpfMessageParserFactory.getParser(ConfigFactory.create(OdpfSinkConfig.class, System.getenv()), statsDReporter), ConfigFactory.create(OdpfSinkConfig.class, System.getenv()));
        this.statsDReporter = statsDReporter;
        this.redisSinkConfig = redisSinkConfig;
        this.protoToFieldMapper = protoToFieldMapper;
    }

    @Override
    public List<RedisDataEntry> parse(OdpfMessage message) throws IOException {
        ParsedOdpfMessage parsedMessage = parseEsbMessage(message);
        String redisKey = parseTemplate(parsedMessage, redisSinkConfig.getSinkRedisKeyTemplate());
        List<RedisDataEntry> messageEntries = new ArrayList<>();
        Map<String, Object> protoToFieldMap = protoToFieldMapper.getFields(getPayload(message));
        protoToFieldMap.forEach((key, value) -> messageEntries.add(new RedisHashSetFieldEntry(redisKey, parseTemplate(parsedMessage, key), String.valueOf(value), new FirehoseInstrumentation(statsDReporter, RedisHashSetFieldEntry.class))));
        return messageEntries;
    }
}
