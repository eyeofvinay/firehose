package io.odpf.depot.redis;

import io.odpf.depot.OdpfSink;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.config.RedisSinkConfig;
import io.odpf.firehose.metrics.FirehoseInstrumentation;
import io.odpf.depot.redis.client.RedisClient;
import io.odpf.depot.redis.client.RedisClientFactory;
import io.odpf.stencil.client.StencilClient;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

/**
 * Factory class to create the RedisSink.
 * <p>
 * The firehose would reflectively instantiate this factory
 * using the configurations supplied and invoke {@see #create(Map < String, String > configuration, StatsDClient statsDReporter, StencilClient client)}
 * to obtain the RedisSink implementation.
 */
public class RedisSinkFactory {

    /**
     * Creates Redis sink.
     *
     * @param configuration  the configuration
     * @param statsDReporter the stats d reporter
     * @param stencilClient  the stencil client
     * @return the abstract sink
     */
    public static OdpfSink create(Map<String, String> configuration, StatsDReporter statsDReporter, StencilClient stencilClient) {
        RedisSinkConfig redisSinkConfig = ConfigFactory.create(RedisSinkConfig.class, configuration);
        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, RedisSinkFactory.class);
        String redisConfig = String.format("\n\tredis.urls = %s\n\tredis.key.template = %s\n\tredis.sink.type = %s"
                        + "\n\tredis.list.data.proto.index = %s\n\tredis.ttl.type = %s\n\tredis.ttl.value = %d",
                redisSinkConfig.getSinkRedisUrls(),
                redisSinkConfig.getSinkRedisKeyTemplate(),
                redisSinkConfig.getSinkRedisDataType().toString(),
                redisSinkConfig.getSinkRedisListDataProtoIndex(),
                redisSinkConfig.getSinkRedisTtlType().toString(),
                redisSinkConfig.getSinkRedisTtlValue());
        firehoseInstrumentation.logDebug(redisConfig);
        firehoseInstrumentation.logInfo("Redis server type = {}", redisSinkConfig.getSinkRedisDeploymentType());

        RedisClientFactory redisClientFactory = new RedisClientFactory(statsDReporter, redisSinkConfig, stencilClient);
        RedisClient client = redisClientFactory.getClient();
        firehoseInstrumentation.logInfo("Connection to redis established successfully");
        return new RedisSink(new FirehoseInstrumentation(statsDReporter, RedisSink.class), "redis", client);
    }
}
