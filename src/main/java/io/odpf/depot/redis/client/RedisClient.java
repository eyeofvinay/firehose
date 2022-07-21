package io.odpf.depot.redis.client;

import io.odpf.depot.message.OdpfMessage;

import java.util.List;

/**
 * Redis client interface to be used in RedisSink.
 */
public interface RedisClient {
    /**
     * Process messages before sending.
     *
     * @param messages the messages
     */
    void prepare(List<OdpfMessage> messages);

    /**
     * Sends the processed messages to redis.
     *
     * @return list of messages
     */
    List<OdpfMessage> execute();

    /**
     * Close the client.
     */
    void close();
}
