package com.linkedin.identity.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.configs.CorpGroupSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.search.CorpGroupDocument;
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
public class CorpGroupSearchDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "corpGroupSearchDAO")
  @DependsOn({"elasticSearchRestHighLevelClient"})
  @Nonnull
  protected ESSearchDAO createInstance() throws IOException {
    return new ESSearchDAO(applicationContext.getBean(RestHighLevelClient.class), CorpGroupDocument.class,
        new CorpGroupSearchConfig(), new ObjectMapper().readTree(new ClassPathResource("corpuser-index-config.json").getInputStream()));
  }
}