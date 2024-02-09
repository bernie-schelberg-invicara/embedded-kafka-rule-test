package com.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.ClassRule;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.hasSize;
import static org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps;

public abstract class AbstractEmbeddedKafkaRuleTest {

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, 1);

    private static final int LOAD_FACTOR = 64;

    @Before
    public void addTopics() {
        EmbeddedKafkaBroker kafkaBroker = embeddedKafka.getEmbeddedKafka();
        String[] topics =
                Stream.generate(() -> "test-topic-" + UUID.randomUUID())
                        .limit(LOAD_FACTOR)
                        .peek(System.out::println)
                        .toArray(String[]::new);
        kafkaBroker.addTopics(topics);

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaBroker);
        ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);

        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(pf);
        Arrays.stream(topics).forEach(topic -> {
            for (int i = 0; i < LOAD_FACTOR; i++) {
                kafkaTemplate.send(topic, UUID.randomUUID().toString());
            }
        });

        Arrays.stream(topics).forEach(topic -> {

            Map<String, Object> metricConsumerProps =
                    consumerProps("metric-consumer-" + UUID.randomUUID(), "false", kafkaBroker);
            DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(metricConsumerProps);
            Consumer<String, String> topicConsumer = cf.createConsumer();
            kafkaBroker.consumeFromAnEmbeddedTopic(topicConsumer, topic);

            Set<TopicPartition> metricTopicPartition = Set.of(new TopicPartition(topic, 0));
            await().until(() -> {
                        topicConsumer.seekToBeginning(metricTopicPartition);

                        return
                                Stream.generate(() -> KafkaTestUtils.getRecords(topicConsumer).records(topic))
                                        .map(Iterable::spliterator)
                                        .flatMap(s -> StreamSupport.stream(s, false))
                                        .limit(LOAD_FACTOR)
                                        .map(ConsumerRecord::value)
                                        .collect(Collectors.toList());
                    },
                    hasSize(LOAD_FACTOR));
            topicConsumer.close();
        });
    }

}
