package com.linkedin.dataset.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.configs.DatasetSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.search.DatasetDocument;
import java.io.IOException;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import javax.annotation.Nonnull;
import org.springframework.core.io.ClassPathResource;

@Configuration
public class DatasetSearchDAOFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Nonnull
  @DependsOn({"elasticSearchRestHighLevelClient"})
  @Bean(name = "datasetSearchDao")
  protected ESSearchDAO createInstance() throws IOException {
    return new ESSearchDAO(applicationContext.getBean(RestHighLevelClient.class), DatasetDocument.class,
        new DatasetSearchConfig(), new ObjectMapper().readTree(new ClassPathResource("dataset-index-config.json").getInputStream()));
  }
}