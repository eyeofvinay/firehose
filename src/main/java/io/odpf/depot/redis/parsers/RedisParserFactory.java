package io.odpf.depot.redis.parsers;

import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.firehose.proto.ProtoToFieldMapper;
import io.odpf.stencil.Parser;

/**
 * Redis parser factory.
 */
public class RedisParserFactory {

    /**
     * Gets parser.
     *
     * @param protoToFieldMapper the proto to field mapper
     * @param protoParser        the proto parser
     * @param redisSinkConfig    the redis sink config
     * @param statsDReporter     the statsd reporter
     * @return RedisParser
     */
    public static RedisParser getParser(ProtoToFieldMapper protoToFieldMapper, RedisSinkConfig redisSinkConfig, StatsDReporter statsDReporter) {

        switch (redisSinkConfig.getSinkRedisDataType()) {
            case LIST:
                return new RedisListParser(redisSinkConfig, statsDReporter);
            case HASHSET:
                return new RedisHashSetParser(protoToFieldMapper, redisSinkConfig, statsDReporter);
            case KEYVALUE:
                return new RedisKeyValueParser(redisSinkConfig, statsDReporter);
            default:
                return null;
        }
    }
}
