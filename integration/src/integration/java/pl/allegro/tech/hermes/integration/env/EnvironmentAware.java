package pl.allegro.tech.hermes.integration.env;

import com.netflix.config.DynamicPropertyFactory;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;

public interface EnvironmentAware {

    ConfigFactory CONFIG_FACTORY = new ConfigFactory(DynamicPropertyFactory.getInstance());

    int HTTP_ENDPOINT_PORT = 18081;

    int MANAGEMENT_PORT = 18082;

    String HTTP_ENDPOINT_URL = "http://localhost:" + HTTP_ENDPOINT_PORT + "/";

    String GOOGLE_PUBSUB_ENDPOINT_URL = "googlepubsub://pubsub.googleapis.com:443/projects/test-project/topics/test-topic";

    String GOOGLE_PUBSUB_PROJECT_ID = "test-project";

    String GOOGLE_PUBSUB_TOPIC_ID = "test-topic";

    String GOOGLE_PUBSUB_SUBSCRIPTION_ID = "test-subscription";

    int FRONTEND_PORT = CONFIG_FACTORY.getIntProperty(Configs.FRONTEND_PORT);

    String FRONTEND_URL = "http://localhost:" + FRONTEND_PORT + "/";

    String FRONTEND_TOPICS_ENDPOINT = FRONTEND_URL + "topics";

    String MANAGEMENT_ENDPOINT_URL = "http://localhost:" + MANAGEMENT_PORT + "/";

    String CONSUMER_ENDPOINT_URL = "http://localhost:" + CONFIG_FACTORY.getIntProperty(Configs.CONSUMER_HEALTH_CHECK_PORT) + "/";

    int GRAPHITE_HTTP_SERVER_PORT = 18089;

    int GRAPHITE_SERVER_PORT = 18023;

    int OAUTH_SERVER_PORT = 19999;

    int AUDIT_EVENT_PORT = 19998;

    String PRIMARY_KAFKA_CLUSTER_NAME = CONFIG_FACTORY.getStringProperty(Configs.KAFKA_CLUSTER_NAME);

    String SECONDARY_KAFKA_CLUSTER_NAME = "secondary";

    String KAFKA_NAMESPACE = CONFIG_FACTORY.getStringProperty(Configs.KAFKA_NAMESPACE);
}
