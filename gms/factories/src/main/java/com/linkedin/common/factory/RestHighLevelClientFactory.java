package com.linkedin.common.factory;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Slf4j
@Configuration
public class RestHighLevelClientFactory {

  @Value("${ELASTICSEARCH_HOST:localhost}")
  private String elasticSearchHost;

  @Value("${ELASTICSEARCH_PORT:9200}")
  private Integer elasticSearchPort;

  @Value("${ELASTICSEARCH_THREAD_COUNT:1}")
  private Integer elasticSearchThreadCount;

  @Value("${ELASTICSEARCH_CONNECTION_REQUEST_TIMEOUT:0}")
  private Integer elasticSearchConectionRequestTimeout;

  @Value("${ELASTICSEARCH_USERNAME:1}")
  private String elasticSearchUser;

  @Value("${ELASTICSEARCH_PASSWORD:1}")
  private String elasticSearchPassword;

  @Bean(name = "elasticSearchRestHighLevelClient")
  @Nonnull
  protected RestHighLevelClient createInstance() {
    try {
      RestClientBuilder restClientBuilder = loadRestHttpClientBuilder(
              elasticSearchHost,
              elasticSearchPort,
              elasticSearchThreadCount,
              elasticSearchConectionRequestTimeout,
          "",
          ""
      );

      return new RestHighLevelClient(restClientBuilder);
    } catch (Exception e) {
      throw new RuntimeException("Error: RestClient is not properly initialized. " + e.toString());
    }
  }

  @Nonnull
  private static RestClient loadRestHttpClient(@Nonnull String host, int port, int threadCount,
                                               int connectionRequestTimeout) throws Exception {
    RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"))
            .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultIOReactorConfig(IOReactorConfig.custom()
                            .setIoThreadCount(threadCount).build()));

    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.
            setConnectionRequestTimeout(connectionRequestTimeout));

    return builder.build();
  }

  @Nonnull
  private static RestClientBuilder loadRestHttpClientBuilder(@Nonnull String host, int port, int threadCount,
      int connectionRequestTimeout, String user, String password) throws Exception {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
    RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"))
        .setHttpClientConfigCallback(httpAsyncClientBuilder ->
            httpAsyncClientBuilder.setDefaultIOReactorConfig(IOReactorConfig.custom()
                .setIoThreadCount(threadCount).build()));

    builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.
        setConnectionRequestTimeout(connectionRequestTimeout));

    return builder;
  }
}

