package pl.allegro.tech.hermes.management.infrastructure.kafka.service;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.common.kafka.ConsumerGroupId;
import pl.allegro.tech.hermes.common.kafka.KafkaNamesMapper;
import pl.allegro.tech.hermes.management.config.kafka.KafkaProperties;
import pl.allegro.tech.hermes.management.domain.subscription.ConsumerGroupManager;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.config.SslConfigs.SSL_CIPHER_SUITES_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_KEY_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_TYPE_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROVIDER_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;

public class KafkaConsumerGroupManager implements ConsumerGroupManager {

    private final Logger logger = LoggerFactory.getLogger(KafkaConsumerGroupManager.class);

    private final KafkaNamesMapper kafkaNamesMapper;
    private final String clusterName;
    private final String bootstrapKafkaServer;
    private final KafkaProperties kafkaProperties;

    public KafkaConsumerGroupManager(KafkaNamesMapper kafkaNamesMapper,
                                     String clusterName,
                                     String bootstrapKafkaServer,
                                     KafkaProperties kafkaProperties) {
        this.kafkaNamesMapper = kafkaNamesMapper;
        this.clusterName = clusterName;
        this.bootstrapKafkaServer = bootstrapKafkaServer;
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public void createConsumerGroup(Topic topic, Subscription subscription) {
        logger.info("Creating consumer group for subscription {}, cluster: {}", subscription.getQualifiedName(), clusterName);

        ConsumerGroupId groupId = kafkaNamesMapper.toConsumerGroupId(subscription.getQualifiedName());
        KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(properties(groupId));

        try {
            String kafkaTopicName = kafkaNamesMapper.toKafkaTopics(topic).getPrimary().name().asString();
            Set<TopicPartition> topicPartitions = kafkaConsumer.partitionsFor(kafkaTopicName).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .collect(toSet());

            logger.info("Received partitions: {}, cluster: {}", topicPartitions, clusterName);

            kafkaConsumer.assign(topicPartitions);

            Map<TopicPartition, OffsetAndMetadata> topicPartitionByOffset = topicPartitions.stream()
                    .map(topicPartition -> {
                        long offset = kafkaConsumer.position(topicPartition);
                        return ImmutablePair.of(topicPartition, new OffsetAndMetadata(offset));
                    })
                    .collect(toMap(Pair::getKey, Pair::getValue));

            kafkaConsumer.commitSync(topicPartitionByOffset);
            kafkaConsumer.close();

            logger.info("Successfully created consumer group for subscription {}, cluster: {}",
                    subscription.getQualifiedName(), clusterName);
        } catch (Exception e) {
            logger.error("Failed to create consumer group for subscription {}, cluster: {}",
                    subscription.getQualifiedName(), clusterName, e);
        }
    }

    private Properties properties(ConsumerGroupId groupId) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapKafkaServer);
        props.put(GROUP_ID_CONFIG, groupId.asString());
        props.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(SECURITY_PROTOCOL_CONFIG, kafkaProperties.getSecurityProtocol());

        if (kafkaProperties.getSasl().isEnabled()) {
            props.put(SASL_MECHANISM, kafkaProperties.getSasl().getMechanism());
            props.put(SASL_JAAS_CONFIG, kafkaProperties.getSasl().getJaasConfig());
        }

        if (kafkaProperties.getSsl().isEnabled()) {
            props.put(SSL_KEY_PASSWORD_CONFIG, kafkaProperties.getSsl().getKeyPassword());
            props.put(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, kafkaProperties.getSsl().getKeyStoreCertificateChain());
            props.put(SSL_KEYSTORE_KEY_CONFIG, kafkaProperties.getSsl().getKeyStoreKey());
            props.put(SSL_KEYSTORE_LOCATION_CONFIG, kafkaProperties.getSsl().getKeyStoreLocation());
            props.put(SSL_KEYSTORE_PASSWORD_CONFIG, kafkaProperties.getSsl().getKeyStorePassword());
            props.put(SSL_TRUSTSTORE_CERTIFICATES_CONFIG, kafkaProperties.getSsl().getTrustStoreCertificates());
            props.put(SSL_TRUSTSTORE_LOCATION_CONFIG, kafkaProperties.getSsl().getTrustStoreLocation());
            props.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, kafkaProperties.getSsl().getTrustStorePassword());
            props.put(SSL_ENABLED_PROTOCOLS_CONFIG, Optional.of(kafkaProperties.getSsl().getEnabledProtocols()).map(s -> Arrays.asList(s.split(","))).orElse(null));
            props.put(SSL_KEYSTORE_TYPE_CONFIG, kafkaProperties.getSsl().getKeyStoreType());
            props.put(SSL_PROTOCOL_CONFIG, kafkaProperties.getSsl().getProtocolVersion());
            props.put(SSL_PROVIDER_CONFIG, kafkaProperties.getSsl().getProvider());
            props.put(SSL_TRUSTSTORE_TYPE_CONFIG, kafkaProperties.getSsl().getTrustStoreType());
            props.put(SSL_CIPHER_SUITES_CONFIG, Optional.of(kafkaProperties.getSsl().getCipherSuites()).map(s -> Arrays.asList(s.split(","))).orElse(null));
            props.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, kafkaProperties.getSsl().getEndpointIdentificationAlgorithm());
            props.put(SSL_ENGINE_FACTORY_CLASS_CONFIG, kafkaProperties.getSsl().getEngineFactoryClass());
            props.put(SSL_KEYMANAGER_ALGORITHM_CONFIG, kafkaProperties.getSsl().getKeymanagerAlgorithm());
            props.put(SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, kafkaProperties.getSsl().getSecureRandomImplementation());
            props.put(SSL_TRUSTMANAGER_ALGORITHM_CONFIG, kafkaProperties.getSsl().getTrustmanagerAlgorithm());
        }
        return props;
    }
}
