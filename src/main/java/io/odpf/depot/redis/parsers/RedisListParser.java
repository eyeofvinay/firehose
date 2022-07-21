package io.odpf.depot.redis.parsers;


import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.redis.dataentry.RedisDataEntry;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.firehose.message.Message;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.depot.redis.dataentry.RedisListEntry;
import com.google.protobuf.DynamicMessage;
import io.odpf.stencil.Parser;
import org.aeonbits.owner.ConfigFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Redis list parser.
 */
public class RedisListParser extends RedisParser {
    private RedisSinkConfig redisSinkConfig;
    private StatsDReporter statsDReporter;

    /**
     * Instantiates a new Redis list parser.
     *
     * @param redisSinkConfig the redis sink config
     * @param statsDReporter  the stats d reporter
     */
    public RedisListParser(RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {
        super(OdpfMessageParserFactory.getParser(ConfigFactory.create(OdpfSinkConfig.class, System.getenv()), statsDReporter), redisSinkConfig);
        this.redisSinkConfig = redisSinkConfig;
        this.statsDReporter = statsDReporter;
    }

    @Override
    public List<RedisDataEntry> parse(OdpfMessage message) {
        ParsedOdpfMessage parsedMessage = parseEsbMessage(message);
        String redisKey = parseTemplate(parsedMessage, redisSinkConfig.getSinkRedisKeyTemplate());
        String protoIndex = redisSinkConfig.getSinkRedisListDataProtoIndex();
        if (protoIndex == null) {
            throw new IllegalArgumentException("Please provide SINK_REDIS_LIST_DATA_PROTO_INDEX in list sink");
        }
        List<RedisDataEntry> messageEntries = new ArrayList<>();
        messageEntries.add(new RedisListEntry(redisKey, getDataByFieldNumber(parsedMessage, protoIndex).toString(), new FirehoseInstrumentation(statsDReporter, RedisListEntry.class)));
        return messageEntries;
    }
}
