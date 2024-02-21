package com.chaolj.core.elasticsearchProvider.AutoConfig;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "elasticsearch")
public class ElasticsearchProperties {
    private String host = "192.168.20.196:9200";
    private String username = "elastic";
    private String password = "qwe_123";
    private int connectionRequestTimeout = 10000;
    private int connectTimeout = 10000;
    private int socketTimeout = 60000;
}
