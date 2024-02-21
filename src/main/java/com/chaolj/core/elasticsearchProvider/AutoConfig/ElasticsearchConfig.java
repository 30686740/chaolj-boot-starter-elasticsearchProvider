package com.chaolj.core.elasticsearchProvider.AutoConfig;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@EnableConfigurationProperties(ElasticsearchProperties.class)
public class ElasticsearchConfig {
    @Autowired
    ApplicationContext applicationContext;
    @Autowired
    ElasticsearchProperties properties;

    private HttpHost[] createHosts(String uris) {
        var uriList = StrUtil.split(uris, ",");
        HttpHost[] httpHosts = new HttpHost[uriList.size()];

        for (int i = 0; i < uriList.size(); i++) {
            String hostStr = uriList.get(i);
            String[] host = hostStr.split(":");
            httpHosts[i] = new HttpHost(StrUtil.trim(host[0]),Integer.valueOf(StrUtil.trim(host[1])));
        }
        return httpHosts;
    }

    @Bean(name = "restHighLevelClient", destroyMethod = "close")
    public RestHighLevelClient restHighLevelClient() {
        var uris = this.properties.getHost();
        var username = this.properties.getUsername();
        var password = this.properties.getPassword();

        var restClient = RestClient.builder(this.createHosts(uris));

        restClient.setHttpClientConfigCallback(config -> {
            var credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            config.setDefaultCredentialsProvider(credentialsProvider);
            return config;
        });

        restClient.setRequestConfigCallback(config -> {
            config.setConnectionRequestTimeout(this.properties.getConnectionRequestTimeout());
            config.setConnectTimeout(this.properties.getConnectTimeout());
            config.setSocketTimeout(this.properties.getSocketTimeout());
            return config;
        });

        return new RestHighLevelClient(restClient);
    }
}
