package com.gojek.esb.factory;

import java.util.Map;
import java.util.Properties;

import com.gojek.esb.config.KafkaConsumerConfig;
import com.gojek.esb.config.DlqConfig;
import com.gojek.esb.consumer.GenericConsumer;
import com.gojek.esb.consumer.Offsets;
import com.gojek.esb.consumer.TopicOffsets;
import com.gojek.esb.consumer.TopicPartitionOffsets;
import com.gojek.esb.filter.Filter;
import com.gojek.esb.metrics.Instrumentation;
import com.gojek.esb.metrics.StatsDReporter;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import io.opentracing.Tracer;
import io.opentracing.contrib.kafka.TracingKafkaConsumer;

/**
 * A factory class to instantiate a kafka consumer.
 */
public class GenericKafkaFactory {

    /**
     * method to create the {@link GenericConsumer} from the parameters supplied.
     *
     * @param config               {@see KafkaConsumerConfig}
     * @param extraKafkaParameters a map containing kafka configurations available as a key/value pair.
     * @param statsDReporter       {@see StatsDClient}
     * @param filter               {@see Filter}, {@see com.gojek.esb.filter.EsbMessageFilter}
     * @return {@see EsbGenericConsumer}
     */
    public GenericConsumer createConsumer(KafkaConsumerConfig config, Map<String, String> extraKafkaParameters,
                                          StatsDReporter statsDReporter, Filter filter, Tracer tracer) {

        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(FactoryUtil.getConfig(config, extraKafkaParameters));
        FactoryUtil.configureSubscription(config, kafkaConsumer, statsDReporter);
        Offsets offsets = !config.isSourceKafkaCommitOnlyCurrentPartitionsEnable()
                ? new TopicOffsets(kafkaConsumer, config, new Instrumentation(statsDReporter, TopicOffsets.class))
                : new TopicPartitionOffsets(kafkaConsumer, config, new Instrumentation(statsDReporter, TopicPartitionOffsets.class));
        TracingKafkaConsumer<byte[], byte[]> tracingKafkaConsumer = new TracingKafkaConsumer<>(kafkaConsumer, tracer);
        return new GenericConsumer(
            tracingKafkaConsumer,
            config,
            filter,
            offsets,
            new Instrumentation(statsDReporter, GenericConsumer.class));
    }

    public KafkaProducer<byte[], byte[]> getKafkaProducer(DlqConfig config) {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getDlqKafkaBrokers());
        props.put("acks", config.getDlqKafkaAcks());
        props.put("retries", config.getDlqKafkaRetries());
        props.put("batch.size", config.getDlqKafkaBatchSize());
        props.put("linger.ms", config.getDlqKafkaLingerMs());
        props.put("buffer.memory", config.getDlqKafkaBufferMemory());
        props.put("key.serializer", config.getDlqKafkaKeySerializer());
        props.put("value.serializer", config.getDlqKafkaValueSerializer());

        return new KafkaProducer<>(props);
    }
}
