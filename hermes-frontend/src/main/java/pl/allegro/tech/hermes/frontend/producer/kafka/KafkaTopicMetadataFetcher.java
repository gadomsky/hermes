package pl.allegro.tech.hermes.frontend.producer.kafka;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.jetbrains.annotations.NotNull;
import pl.allegro.tech.hermes.common.config.ConfigFactory;

import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.apache.kafka.common.config.TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;
import static pl.allegro.tech.hermes.common.config.Configs.KAFKA_PRODUCER_METADATA_MAX_AGE;

public class KafkaTopicMetadataFetcher {
    private final LoadingCache<String, Integer> minInSyncReplicasCache;
    private final AdminClient adminClient;

    KafkaTopicMetadataFetcher(AdminClient adminClient, ConfigFactory configFactory) {
        this.adminClient = adminClient;
        int metadataMaxAgeInMs = configFactory.getIntProperty(KAFKA_PRODUCER_METADATA_MAX_AGE);
        this.minInSyncReplicasCache = CacheBuilder
                .newBuilder()
                .expireAfterWrite(metadataMaxAgeInMs, MILLISECONDS)
                .build(new MinInSyncReplicasLoader());
    }

    int fetchMinInSyncReplicas(String kafkaTopicName) throws Exception {
        return minInSyncReplicasCache.get(kafkaTopicName);
    }

    void close() {
        adminClient.close();
    }

    private class MinInSyncReplicasLoader extends CacheLoader<String, Integer> {

        @Override
        public Integer load(@NotNull String kafkaTopicName) throws Exception {
            ConfigResource resource = new ConfigResource(TOPIC, kafkaTopicName);
            DescribeConfigsResult describeTopicsResult = adminClient.describeConfigs(ImmutableList.of(resource));
            Map<ConfigResource, Config> configMap = describeTopicsResult.all().get();
            Config config = configMap.get(resource);
            ConfigEntry configEntry = config.get(MIN_IN_SYNC_REPLICAS_CONFIG);
            String value = configEntry.value();
            return Integer.parseInt(value);
        }
    }
}
