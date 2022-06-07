package pl.allegro.tech.hermes.consumers.consumer.receiver.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.kafka.ConsumerGroupId;
import pl.allegro.tech.hermes.common.kafka.KafkaNamesMapper;
import pl.allegro.tech.hermes.common.metric.HermesMetrics;
import pl.allegro.tech.hermes.consumers.consumer.filtering.FilteredMessageHandler;
import pl.allegro.tech.hermes.consumers.consumer.idleTime.ExponentiallyGrowingIdleTimeCalculator;
import pl.allegro.tech.hermes.consumers.consumer.idleTime.IdleTimeCalculator;
import pl.allegro.tech.hermes.consumers.consumer.offset.ConsumerPartitionAssignmentState;
import pl.allegro.tech.hermes.consumers.consumer.offset.OffsetQueue;
import pl.allegro.tech.hermes.consumers.consumer.rate.ConsumerRateLimiter;
import pl.allegro.tech.hermes.consumers.consumer.receiver.MessageReceiver;
import pl.allegro.tech.hermes.consumers.consumer.receiver.ReceiverFactory;
import pl.allegro.tech.hermes.consumers.consumer.receiver.ThrottlingMessageReceiver;
import pl.allegro.tech.hermes.domain.filtering.chain.FilterChainFactory;
import pl.allegro.tech.hermes.tracker.consumers.Trackers;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CHECK_CRCS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.METADATA_MAX_AGE_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RECEIVE_BUFFER_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SEND_BUFFER_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_JAAS_CONFIG;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;
import static org.apache.kafka.common.config.SslConfigs.SSL_PROTOCOL_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.*;
import static pl.allegro.tech.hermes.common.config.Configs.*;

public class KafkaMessageReceiverFactory implements ReceiverFactory {

    private final ConfigFactory configs;
    private final KafkaConsumerRecordToMessageConverterFactory messageConverterFactory;
    private final HermesMetrics hermesMetrics;
    private final OffsetQueue offsetQueue;
    private final KafkaNamesMapper kafkaNamesMapper;
    private final FilterChainFactory filterChainFactory;
    private final Trackers trackers;
    private final ConsumerPartitionAssignmentState consumerPartitionAssignmentState;

    public KafkaMessageReceiverFactory(ConfigFactory configs,
                                       KafkaConsumerRecordToMessageConverterFactory messageConverterFactory,
                                       HermesMetrics hermesMetrics,
                                       OffsetQueue offsetQueue,
                                       KafkaNamesMapper kafkaNamesMapper,
                                       FilterChainFactory filterChainFactory,
                                       Trackers trackers,
                                       ConsumerPartitionAssignmentState consumerPartitionAssignmentState) {
        this.configs = configs;
        this.messageConverterFactory = messageConverterFactory;
        this.hermesMetrics = hermesMetrics;
        this.offsetQueue = offsetQueue;
        this.kafkaNamesMapper = kafkaNamesMapper;
        this.filterChainFactory = filterChainFactory;
        this.trackers = trackers;
        this.consumerPartitionAssignmentState = consumerPartitionAssignmentState;
    }

    @Override
    public MessageReceiver createMessageReceiver(Topic topic,
                                                 Subscription subscription,
                                                 ConsumerRateLimiter consumerRateLimiter) {

        MessageReceiver receiver = new KafkaSingleThreadedMessageReceiver(
                createKafkaConsumer(topic, subscription),
                messageConverterFactory,
                hermesMetrics,
                kafkaNamesMapper,
                topic,
                subscription,
                configs.getIntProperty(Configs.CONSUMER_RECEIVER_POOL_TIMEOUT),
                configs.getIntProperty(Configs.CONSUMER_RECEIVER_READ_QUEUE_CAPACITY),
                consumerPartitionAssignmentState);


        if (configs.getBooleanProperty(Configs.CONSUMER_RECEIVER_WAIT_BETWEEN_UNSUCCESSFUL_POLLS)) {
            IdleTimeCalculator idleTimeCalculator = new ExponentiallyGrowingIdleTimeCalculator(
                    configs.getIntProperty(CONSUMER_RECEIVER_INITIAL_IDLE_TIME),
                    configs.getIntProperty(CONSUMER_RECEIVER_MAX_IDLE_TIME)
            );
            receiver = new ThrottlingMessageReceiver(receiver, idleTimeCalculator, subscription, hermesMetrics);
        }

        boolean filteringRateLimitEnabled = configs.getBooleanProperty(Configs.CONSUMER_FILTERING_RATE_LIMITER_ENABLED);
        if (configs.getBooleanProperty(Configs.CONSUMER_FILTERING_ENABLED)) {
            FilteredMessageHandler filteredMessageHandler = new FilteredMessageHandler(
                    offsetQueue,
                    filteringRateLimitEnabled ? consumerRateLimiter : null,
                    trackers,
                    hermesMetrics);
            receiver = new FilteringMessageReceiver(receiver, filteredMessageHandler, filterChainFactory, subscription);
        }
        return receiver;
    }

    private KafkaConsumer<byte[], byte[]> createKafkaConsumer(Topic topic, Subscription subscription) {
        ConsumerGroupId groupId = kafkaNamesMapper.toConsumerGroupId(subscription.getQualifiedName());
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, configs.getStringProperty(Configs.KAFKA_BROKER_LIST));
        props.put(CLIENT_ID_CONFIG, configs.getStringProperty(Configs.CONSUMER_CLIENT_ID) + "_" + groupId.asString());
        props.put(GROUP_ID_CONFIG, groupId.asString());
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        props.put(SECURITY_PROTOCOL_CONFIG, configs.getStringProperty(KAFKA_SECURITY_PROTOCOL));

        if(configs.getBooleanProperty(KAFKA_AUTHORIZATION_ENABLED)){
            props.put(SASL_MECHANISM, configs.getStringProperty(KAFKA_AUTHORIZATION_MECHANISM));
            props.put(SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                            + "username=\"" + configs.getStringProperty(KAFKA_AUTHORIZATION_USERNAME) + "\"\n"
                            + "password=\"" + configs.getStringProperty(KAFKA_AUTHORIZATION_PASSWORD) + "\";"
            );
        }

        if(configs.getBooleanProperty(KAFKA_SSL_ENABLED)){
            props.put(SSL_KEY_PASSWORD_CONFIG, configs.getStringProperty(KAFKA_SSL_KEY_PASSWORD));
            props.put(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, configs.getStringProperty(KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN));
            props.put(SSL_KEYSTORE_KEY_CONFIG, configs.getStringProperty(KAFKA_SSL_KEYSTORE_KEY));
            props.put(SSL_KEYSTORE_LOCATION_CONFIG, configs.getStringProperty(KAFKA_SSL_KEYSTORE_LOCATION));
            props.put(SSL_KEYSTORE_PASSWORD_CONFIG, configs.getStringProperty(KAFKA_SSL_KEYSTORE_PASSWORD));
            props.put(SSL_TRUSTSTORE_CERTIFICATES_CONFIG, configs.getStringProperty(KAFKA_SSL_TRUSTSTORE_CERTIFICATES));
            props.put(SSL_TRUSTSTORE_LOCATION_CONFIG, configs.getStringProperty(KAFKA_SSL_TRUSTSTORE_LOCATION));
            props.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, configs.getStringProperty(KAFKA_SSL_TRUSTSTORE_PASSWORD));
            props.put(SSL_ENABLED_PROTOCOLS_CONFIG, Optional.of(configs.getStringProperty(KAFKA_SSL_ENABLED_PROTOCOLS)).map(s -> Arrays.asList(s.split(","))).orElse(null));
            props.put(SSL_KEYSTORE_TYPE_CONFIG, configs.getStringProperty(KAFKA_SSL_KEYSTORE_TYPE));
            props.put(SSL_PROTOCOL_CONFIG, configs.getStringProperty(KAFKA_SSL_PROTOCOL));
            props.put(SSL_PROVIDER_CONFIG, configs.getStringProperty(KAFKA_SSL_PROVIDER));
            props.put(SSL_TRUSTSTORE_TYPE_CONFIG, configs.getStringProperty(KAFKA_SSL_TRUSTSTORE_TYPE));
            props.put(SSL_CIPHER_SUITES_CONFIG, Optional.of(configs.getStringProperty(KAFKA_SSL_CIPHER_SUITES)).map(s -> Arrays.asList(s.split(","))).orElse(null));
            props.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, configs.getStringProperty(KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM));
            props.put(SSL_ENGINE_FACTORY_CLASS_CONFIG, configs.getStringProperty(KAFKA_SSL_ENGINE_FACTORY_CLASS));
            props.put(SSL_KEYMANAGER_ALGORITHM_CONFIG, configs.getStringProperty(KAFKA_SSL_KEYMANAGER_ALGORITHM));
            props.put(SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, configs.getStringProperty(KAFKA_SSL_SECURE_RANDOM_IMPLEMENTATION));
            props.put(SSL_TRUSTMANAGER_ALGORITHM_CONFIG, configs.getStringProperty(KAFKA_SSL_TRUSTMANAGER_ALGORITHM));
        }

        props.put(AUTO_OFFSET_RESET_CONFIG, configs.getStringProperty(Configs.KAFKA_CONSUMER_AUTO_OFFSET_RESET_CONFIG));
        props.put(SESSION_TIMEOUT_MS_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_SESSION_TIMEOUT_MS_CONFIG));
        props.put(HEARTBEAT_INTERVAL_MS_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG));
        props.put(METADATA_MAX_AGE_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_METADATA_MAX_AGE_CONFIG));
        props.put(MAX_PARTITION_FETCH_BYTES_CONFIG, getMaxPartitionFetch(topic, configs));
        props.put(SEND_BUFFER_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_SEND_BUFFER_CONFIG));
        props.put(RECEIVE_BUFFER_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_RECEIVE_BUFFER_CONFIG));
        props.put(FETCH_MIN_BYTES_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_FETCH_MIN_BYTES_CONFIG));
        props.put(FETCH_MAX_WAIT_MS_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_FETCH_MAX_WAIT_MS_CONFIG));
        props.put(RECONNECT_BACKOFF_MS_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_RECONNECT_BACKOFF_MS_CONFIG));
        props.put(RETRY_BACKOFF_MS_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_RETRY_BACKOFF_MS_CONFIG));
        props.put(CHECK_CRCS_CONFIG, configs.getBooleanProperty(Configs.KAFKA_CONSUMER_CHECK_CRCS_CONFIG));
        props.put(METRICS_SAMPLE_WINDOW_MS_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_METRICS_SAMPLE_WINDOW_MS_CONFIG));
        props.put(METRICS_NUM_SAMPLES_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_METRICS_NUM_SAMPLES_CONFIG));
        props.put(REQUEST_TIMEOUT_MS_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_REQUEST_TIMEOUT_MS_CONFIG));
        props.put(CONNECTIONS_MAX_IDLE_MS_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_CONNECTIONS_MAX_IDLE_MS_CONFIG));
        props.put(MAX_POLL_RECORDS_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_MAX_POLL_RECORDS_CONFIG));
        props.put(MAX_POLL_INTERVAL_MS_CONFIG, configs.getIntProperty(Configs.KAFKA_CONSUMER_MAX_POLL_INTERVAL_CONFIG));
        return new KafkaConsumer<>(props);
    }

    private int getMaxPartitionFetch(Topic topic, ConfigFactory configs) {
        if (configs.getBooleanProperty(Configs.CONSUMER_USE_TOPIC_MESSAGE_SIZE)) {
            int topicMessageSize = topic.getMaxMessageSize();
            int min = configs.getIntProperty(Configs.KAFKA_CONSUMER_MAX_PARTITION_FETCH_MIN_BYTES_CONFIG);
            int max = configs.getIntProperty(Configs.KAFKA_CONSUMER_MAX_PARTITION_FETCH_MAX_BYTES_CONFIG);
            return Math.max(Math.min(topicMessageSize, max), min);
        } else {
            return configs.getIntProperty(Configs.KAFKA_CONSUMER_MAX_PARTITION_FETCH_MAX_BYTES_CONFIG);
        }
    }
}
