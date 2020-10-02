package com.linkedin.identity.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.configs.CorpUserSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.search.CorpUserInfoDocument;
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
public class CorpUserSearchDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "corpUserSearchDAO")
  @DependsOn({"elasticSearchRestHighLevelClient"})
  @Nonnull
  protected ESSearchDAO createInstance() throws IOException {
    return new ESSearchDAO(applicationContext.getBean(
        RestHighLevelClient.class),
        CorpUserInfoDocument.class,
        new CorpUserSearchConfig(),
        new ObjectMapper().readTree(new ClassPathResource("corpuser-index-config.json").getInputStream()));
  }
}