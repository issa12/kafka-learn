package com.isa.kafka.consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    private final static Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        String groupId = "consumer-opensearch-demo";
        String server = "127.0.0.1:9092";

        String topic = "";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // create consumer
        return new KafkaConsumer<>(properties);

    }

    private static String extractId(String json) {
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        log.info("Open Search Kafka consumer");
        String index = "wikimedia";
        String topic = "wikimedia-recentchange";
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        try (openSearchClient; consumer) {
            boolean indexExist = openSearchClient.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT);
            log.info("Index {} exists ? {}", index, indexExist);
            if (!indexExist) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("index created!");
            }

            final Thread mainThread = Thread.currentThread();

            // add shutdown hook

            Runtime.getRuntime().addShutdownHook(new Thread() {
                                                     public void run() {
                                                         log.info("Shutdown detected, let's exit by consumer");
                                                         consumer.wakeup();
                                                         try {
                                                             mainThread.join();
                                                         } catch (Exception e) {

                                                         }

                                                     }
                                                 }
            );

            consumer.subscribe(Collections.singleton(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
                int count = records.count();
                log.info("records count {}", count);

                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> rec : records) {

                    /* to provid ID to a consumed record: we can use either
                       - Kafka record coordinates: rec.topic()+"-"+rec.partition() + "-" + rec.offset()
                       - of see if the message has an id then use it
                     */

                    // Kafka record coordinates

                    String id = rec.topic() + "-" + rec.partition() + "-" + rec.offset();
                    try {
                        String recId = extractId(rec.value());
                        //log.info("coordinates id {}, meta record ID: {}", id, recId);
                        IndexRequest indexRequest =
                                new IndexRequest(index).source(rec.value(),
                                        XContentType.JSON).id(id);
                        bulkRequest.add(indexRequest);
                        /*
                        IndexResponse response = openSearchClient.index(
                                indexRequest, RequestOptions.DEFAULT);
                                */

                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                    //log.info("Inserted 1 document into OpenSearch, ResponseID {}", response.getId());

                }
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkItemResponses = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("responses {}", bulkItemResponses.getItems().length);

                    try {
                        Thread.sleep(1000);

                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                consumer.commitAsync();
                log.info("ID commited");
            }
        } catch (WakeupException wakeupException ) {
            log.info("Consumer is starting shutdown");
        } catch (Exception e) {
            log.error("Unexpected error");
        }
        finally {
            consumer.close();
            openSearchClient.close();
            log.info("The consumer is gracefully shutdown");
        }

    }
}
