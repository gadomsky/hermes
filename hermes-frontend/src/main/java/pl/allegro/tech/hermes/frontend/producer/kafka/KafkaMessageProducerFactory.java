package pl.allegro.tech.hermes.frontend.producer.kafka;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.METADATA_MAX_AGE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.SEND_BUFFER_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
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
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_ENABLED;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_MECHANISM;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_PASSWORD;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_USERNAME;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_BROKER_LIST;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_PRODUCER_BATCH_SIZE;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_PRODUCER_COMPRESSION_CODEC;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_PRODUCER_LINGER_MS;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_PRODUCER_MAX_BLOCK_MS;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_PRODUCER_MAX_REQUEST_SIZE;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_PRODUCER_METADATA_MAX_AGE;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_PRODUCER_METRICS_SAMPLE_WINDOW_MS;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_PRODUCER_REQUEST_TIMEOUT_MS;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_PRODUCER_RETRIES;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_PRODUCER_RETRY_BACKOFF_MS;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_PRODUCER_TCP_SEND_BUFFER;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SECURITY_PROTOCOL;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_CIPHER_SUITES;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_ENABLED;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_ENABLED_PROTOCOLS;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_ENGINE_FACTORY_CLASS;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_KEYMANAGER_ALGORITHM;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_KEYSTORE_KEY;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_KEYSTORE_LOCATION;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_KEYSTORE_PASSWORD;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_KEYSTORE_TYPE;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_KEY_PASSWORD;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_PROTOCOL;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_PROVIDER;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_SECURE_RANDOM_IMPLEMENTATION;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_TRUSTMANAGER_ALGORITHM;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_TRUSTSTORE_CERTIFICATES;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_TRUSTSTORE_LOCATION;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_TRUSTSTORE_PASSWORD;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_SSL_TRUSTSTORE_TYPE;
import static pl.allegro.tech.hermes.common.config.Configs.MESSAGES_LOCAL_BUFFERED_STORAGE_SIZE;

public class KafkaMessageProducerFactory {
    private static final String ACK_ALL = "-1";
    private static final String ACK_LEADER = "1";

    private final ConfigFactory configFactory;

    public KafkaMessageProducerFactory(ConfigFactory configFactory) {
        this.configFactory = configFactory;
    }

    public Producers provide() {
        Map<String, Object> props = new HashMap<>();
        props.put(BOOTSTRAP_SERVERS_CONFIG, getString(KAFKA_BROKER_LIST));
        props.put(MAX_BLOCK_MS_CONFIG, getInt(KAFKA_PRODUCER_MAX_BLOCK_MS));
        props.put(COMPRESSION_TYPE_CONFIG, getString(KAFKA_PRODUCER_COMPRESSION_CODEC));
        props.put(BUFFER_MEMORY_CONFIG, configFactory.getLongProperty(MESSAGES_LOCAL_BUFFERED_STORAGE_SIZE));
        props.put(REQUEST_TIMEOUT_MS_CONFIG, getInt(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS));
        props.put(BATCH_SIZE_CONFIG, getInt(KAFKA_PRODUCER_BATCH_SIZE));
        props.put(SEND_BUFFER_CONFIG, getInt(KAFKA_PRODUCER_TCP_SEND_BUFFER));
        props.put(RETRIES_CONFIG, getInt(KAFKA_PRODUCER_RETRIES));
        props.put(RETRY_BACKOFF_MS_CONFIG, getInt(KAFKA_PRODUCER_RETRY_BACKOFF_MS));
        props.put(METADATA_MAX_AGE_CONFIG, getInt(KAFKA_PRODUCER_METADATA_MAX_AGE));
        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(MAX_REQUEST_SIZE_CONFIG, getInt(KAFKA_PRODUCER_MAX_REQUEST_SIZE));
        props.put(LINGER_MS_CONFIG, getInt(KAFKA_PRODUCER_LINGER_MS));
        props.put(METRICS_SAMPLE_WINDOW_MS_CONFIG, getInt(KAFKA_PRODUCER_METRICS_SAMPLE_WINDOW_MS));
        props.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, getInt(KAFKA_PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));

        props.put(SECURITY_PROTOCOL_CONFIG, getString(KAFKA_SECURITY_PROTOCOL));

        if(getBoolean(KAFKA_AUTHORIZATION_ENABLED)){
            props.put(SASL_MECHANISM, getString(KAFKA_AUTHORIZATION_MECHANISM));
            props.put(SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                            + "username=\"" + getString(KAFKA_AUTHORIZATION_USERNAME) + "\"\n"
                            + "password=\"" + getString(KAFKA_AUTHORIZATION_PASSWORD) + "\";"
            );
        }

        if(getBoolean(KAFKA_SSL_ENABLED)){
            props.put(SSL_KEY_PASSWORD_CONFIG, getString(KAFKA_SSL_KEY_PASSWORD));
            props.put(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, getString(KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN));
            props.put(SSL_KEYSTORE_KEY_CONFIG, getString(KAFKA_SSL_KEYSTORE_KEY));
            props.put(SSL_KEYSTORE_LOCATION_CONFIG, getString(KAFKA_SSL_KEYSTORE_LOCATION));
            props.put(SSL_KEYSTORE_PASSWORD_CONFIG, getString(KAFKA_SSL_KEYSTORE_PASSWORD));
            props.put(SSL_TRUSTSTORE_CERTIFICATES_CONFIG, getString(KAFKA_SSL_TRUSTSTORE_CERTIFICATES));
            props.put(SSL_TRUSTSTORE_LOCATION_CONFIG, getString(KAFKA_SSL_TRUSTSTORE_LOCATION));
            props.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, getString(KAFKA_SSL_TRUSTSTORE_PASSWORD));
            props.put(SSL_ENABLED_PROTOCOLS_CONFIG, Optional.of(getString(KAFKA_SSL_ENABLED_PROTOCOLS)).map(s -> Arrays.asList(s.split(","))).orElse(null));
            props.put(SSL_KEYSTORE_TYPE_CONFIG, getString(KAFKA_SSL_KEYSTORE_TYPE));
            props.put(SSL_PROTOCOL_CONFIG, getString(KAFKA_SSL_PROTOCOL));
            props.put(SSL_PROVIDER_CONFIG, getString(KAFKA_SSL_PROVIDER));
            props.put(SSL_TRUSTSTORE_TYPE_CONFIG, getString(KAFKA_SSL_TRUSTSTORE_TYPE));
            props.put(SSL_CIPHER_SUITES_CONFIG, Optional.of(getString(KAFKA_SSL_CIPHER_SUITES)).map(s -> Arrays.asList(s.split(","))).orElse(null));
            props.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, getString(KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM));
            props.put(SSL_ENGINE_FACTORY_CLASS_CONFIG, getString(KAFKA_SSL_ENGINE_FACTORY_CLASS));
            props.put(SSL_KEYMANAGER_ALGORITHM_CONFIG, getString(KAFKA_SSL_KEYMANAGER_ALGORITHM));
            props.put(SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, getString(KAFKA_SSL_SECURE_RANDOM_IMPLEMENTATION));
            props.put(SSL_TRUSTMANAGER_ALGORITHM_CONFIG, getString(KAFKA_SSL_TRUSTMANAGER_ALGORITHM));
        }

        Producer<byte[], byte[]> leaderConfirms = new KafkaProducer<>(copyWithEntryAdded(props, ACKS_CONFIG, ACK_LEADER));
        Producer<byte[], byte[]> everyoneConfirms = new KafkaProducer<>(copyWithEntryAdded(props, ACKS_CONFIG, ACK_ALL));
        return new Producers(leaderConfirms, everyoneConfirms, configFactory);
    }

    private ImmutableMap<String, Object> copyWithEntryAdded(Map<String, Object> common, String key, String value) {
        return ImmutableMap.<String, Object>builder().putAll(common).put(key, value).build();
    }

    private Boolean getBoolean(Configs key) {
        return configFactory.getBooleanProperty(key);
    }

    private String getString(Configs key) {
        return configFactory.getStringProperty(key);
    }

    private Integer getInt(Configs key) {
        return configFactory.getIntProperty(key);
    }
}
