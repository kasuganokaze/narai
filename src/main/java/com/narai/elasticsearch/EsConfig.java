package com.narai.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: kaze
 * @date: 2019-02-20
 */
@Configuration
public class EsConfig {

    @Value("${spring.data.elasticsearch.cluster-nodes}")
    private String nodes;
    @Value("${spring.data.elasticsearch.cluster-name}")
    private String name;

    @Bean
    public RestClient esClient() throws Exception {
        List<HttpHost> collect = Arrays.stream(nodes.split(","))
                .map(e -> {
                    String[] node = e.split(":");
                    return new HttpHost(node[0], Integer.parseInt(node[1]));
                })
                .collect(Collectors.toList());
        HttpHost[] httpHosts = new HttpHost[collect.size()];
        collect.toArray(httpHosts);
        return RestClient.builder(httpHosts).build();
    }

}
