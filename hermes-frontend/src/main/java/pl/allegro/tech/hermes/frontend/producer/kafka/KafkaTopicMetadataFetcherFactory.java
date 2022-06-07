package pl.allegro.tech.hermes.frontend.producer.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import pl.allegro.tech.hermes.common.config.ConfigFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
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
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_ADMIN_REQUEST_TIMEOUT_MS;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_ENABLED;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_MECHANISM;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_PASSWORD;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_AUTHORIZATION_USERNAME;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_BROKER_LIST;
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

public class KafkaTopicMetadataFetcherFactory {
    private final ConfigFactory configFactory;

    public KafkaTopicMetadataFetcherFactory(ConfigFactory configFactory) {
        this.configFactory = configFactory;
    }

    public KafkaTopicMetadataFetcher provide() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, configFactory.getStringProperty(KAFKA_BROKER_LIST));
        props.put(REQUEST_TIMEOUT_MS_CONFIG, configFactory.getIntProperty(KAFKA_ADMIN_REQUEST_TIMEOUT_MS));

        props.put(SECURITY_PROTOCOL_CONFIG, configFactory.getStringProperty(KAFKA_SECURITY_PROTOCOL));

        if(configFactory.getBooleanProperty(KAFKA_AUTHORIZATION_ENABLED)){
            props.put(SASL_MECHANISM, configFactory.getStringProperty(KAFKA_AUTHORIZATION_MECHANISM));
            props.put(SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                            + "username=\"" + configFactory.getStringProperty(KAFKA_AUTHORIZATION_USERNAME) + "\"\n"
                            + "password=\"" + configFactory.getStringProperty(KAFKA_AUTHORIZATION_PASSWORD) + "\";"
            );
        }

        if(configFactory.getBooleanProperty(KAFKA_SSL_ENABLED)){
            props.put(SSL_KEY_PASSWORD_CONFIG, configFactory.getStringProperty(KAFKA_SSL_KEY_PASSWORD));
            props.put(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, configFactory.getStringProperty(KAFKA_SSL_KEYSTORE_CERTIFICATE_CHAIN));
            props.put(SSL_KEYSTORE_KEY_CONFIG, configFactory.getStringProperty(KAFKA_SSL_KEYSTORE_KEY));
            props.put(SSL_KEYSTORE_LOCATION_CONFIG, configFactory.getStringProperty(KAFKA_SSL_KEYSTORE_LOCATION));
            props.put(SSL_KEYSTORE_PASSWORD_CONFIG, configFactory.getStringProperty(KAFKA_SSL_KEYSTORE_PASSWORD));
            props.put(SSL_TRUSTSTORE_CERTIFICATES_CONFIG, configFactory.getStringProperty(KAFKA_SSL_TRUSTSTORE_CERTIFICATES));
            props.put(SSL_TRUSTSTORE_LOCATION_CONFIG, configFactory.getStringProperty(KAFKA_SSL_TRUSTSTORE_LOCATION));
            props.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, configFactory.getStringProperty(KAFKA_SSL_TRUSTSTORE_PASSWORD));
            props.put(SSL_ENABLED_PROTOCOLS_CONFIG, Optional.of(configFactory.getStringProperty(KAFKA_SSL_ENABLED_PROTOCOLS)).map(s -> Arrays.asList(s.split(","))).orElse(null));
            props.put(SSL_KEYSTORE_TYPE_CONFIG, configFactory.getStringProperty(KAFKA_SSL_KEYSTORE_TYPE));
            props.put(SSL_PROTOCOL_CONFIG, configFactory.getStringProperty(KAFKA_SSL_PROTOCOL));
            props.put(SSL_PROVIDER_CONFIG, configFactory.getStringProperty(KAFKA_SSL_PROVIDER));
            props.put(SSL_TRUSTSTORE_TYPE_CONFIG, configFactory.getStringProperty(KAFKA_SSL_TRUSTSTORE_TYPE));
            props.put(SSL_CIPHER_SUITES_CONFIG, Optional.of(configFactory.getStringProperty(KAFKA_SSL_CIPHER_SUITES)).map(s -> Arrays.asList(s.split(","))).orElse(null));
            props.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, configFactory.getStringProperty(KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM));
            props.put(SSL_ENGINE_FACTORY_CLASS_CONFIG, configFactory.getStringProperty(KAFKA_SSL_ENGINE_FACTORY_CLASS));
            props.put(SSL_KEYMANAGER_ALGORITHM_CONFIG, configFactory.getStringProperty(KAFKA_SSL_KEYMANAGER_ALGORITHM));
            props.put(SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, configFactory.getStringProperty(KAFKA_SSL_SECURE_RANDOM_IMPLEMENTATION));
            props.put(SSL_TRUSTMANAGER_ALGORITHM_CONFIG, configFactory.getStringProperty(KAFKA_SSL_TRUSTMANAGER_ALGORITHM));
        }
        AdminClient adminClient = AdminClient.create(props);
        return new KafkaTopicMetadataFetcher(adminClient, configFactory);
    }
}
