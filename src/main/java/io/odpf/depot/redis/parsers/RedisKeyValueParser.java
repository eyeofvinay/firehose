package io.odpf.depot.redis.parsers;

import com.google.protobuf.DynamicMessage;
import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.depot.redis.dataentry.RedisKeyValueEntry;
import io.odpf.stencil.Parser;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class RedisKeyValueParser extends RedisParser {
    private RedisSinkConfig redisSinkConfig;
    private StatsDReporter statsDReporter;

    public RedisKeyValueParser(RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        super(OdpfMessageParserFactory.getParser(ConfigFactory.create(OdpfSinkConfig.class, System.getenv()), statsDReporter), ConfigFactory.create(OdpfSinkConfig.class, System.getenv()));
        this.redisSinkConfig = redisSinkConfig;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<RedisDataEntry> parse(OdpfMessage message) throws IOException {
        ParsedOdpfMessage parsedMessage = parseEsbMessage(message);
        String redisKey = parseTemplate(parsedMessage, redisSinkConfig.getSinkRedisKeyTemplate());
        String protoIndex = redisSinkConfig.getSinkRedisKeyValuetDataProtoIndex();
        if (protoIndex == null) {
            throw new IllegalArgumentException("Please provide SINK_REDIS_KEY_VALUE_DATA_PROTO_INDEX in key value sink");
        }
        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, RedisKeyValueEntry.class);
        RedisKeyValueEntry redisKeyValueEntry = new RedisKeyValueEntry(redisKey, getDataByFieldNumber(parsedMessage, protoIndex).toString(), firehoseInstrumentation);
        return Collections.singletonList(redisKeyValueEntry);
    }
}
