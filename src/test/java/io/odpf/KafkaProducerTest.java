package io.odpf;

import io.odpf.firehose.consumer.TestMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;

class Producer {
    String topicName;
    String bootStrapServer;
    KafkaProducer<byte[], byte[]> producer;
    public Producer(String topicName, String bootStrapServer) {
        this.topicName = topicName;
        this.bootStrapServer = bootStrapServer;

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        //create producer
        producer = new KafkaProducer<>(properties);
    }

    public void produceOne() {
        //build protobuf from String data
        TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("123").setOrderUrl("abc").setOrderDetails("details").build();

        //create a producer record
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topicName, testMessage.toByteArray());

        //sending the data - asynchronous
        producer.send(producerRecord);

        //flush and close client
        producer.close();
    }

    public void produceMany(int N) {
        for(int i = 0; i < N; i++) {
            TestMessage testMessage = TestMessage.newBuilder().setOrderNumber("order" + i).setOrderUrl("url" + i).setOrderDetails("details" + i).build();
            ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topicName, testMessage.toByteArray());
            producer.send(producerRecord);
        }
        producer.close();
    }
}

public class KafkaProducerTest {
    public static void main(String[] args) {
        Producer producer = new Producer("test-topic", "localhost:9092");
        producer.produceMany(4);
    }
}
