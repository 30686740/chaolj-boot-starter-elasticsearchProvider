package com.chaolj.core.elasticsearchProvider;

import com.chaolj.core.commonUtils.myServer.Interface.IElasticsearchServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@EnableConfigurationProperties(MyElasticsearchProviderProperties.class)
public class MyElasticsearchProviderConfig {
    @Autowired
    ApplicationContext applicationContext;
    @Autowired
    MyElasticsearchProviderProperties properties;

    @Bean(name = "myElasticsearchProvider")
    public IElasticsearchServer MyElasticsearchProvider(){
        return new MyElasticsearchProvider(this.applicationContext, this.properties);
    }
}
