package com.chaolj.core.elasticsearchProvider;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "myproviders.myelasticsearchprovider")
public class MyElasticsearchProviderProperties {
    private int requestTimeout = 120000;
    private String idFieldName = "Id";
    private String timeFieldName = "CreateTime";
}
