package com.narai.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.join.query.JoinQueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author: kaze
 * @date: 2019-02-23
 */
@Slf4j
@Component
public class ElasticsearchDemo {

    @Resource
    private RestClient restClient;
    @Resource
    private TransportClient transportClient;
    @Resource
    private ElasticsearchTemplate elasticsearchTemplate;

    @PostConstruct
    public void init() {
        log.info("{}", restClient);
        log.info("{}", transportClient);
        log.info("{}", elasticsearchTemplate);
    }

    public void search() {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.termsQuery("fieldName", "value"));
        boolQuery.must(JoinQueryBuilders.hasChildQuery("type", QueryBuilders.termsQuery("fieldName", "value"), ScoreMode.None));

        AggregationBuilder agg = AggregationBuilders.terms("fieldName");
        agg.subAggregation(AggregationBuilders.terms("fieldName"));

        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch("index")
                .setQuery(boolQuery)
                .setSize(100)
                .addAggregation(agg);

        SearchResponse response = searchRequestBuilder.execute().actionGet(TimeValue.timeValueSeconds(60L));
        SearchHits hits = response.getHits();
        List<JSONObject> list = Stream.of(hits.getHits()).map(r -> JSONObject.parseObject(r.getSourceAsString())).collect(Collectors.toList());
    }

    public void t() throws Exception {

        BulkRequestBuilder bulkRequest = transportClient.prepareBulk();

        JSONObject source = new JSONObject();
        source.put("url", "url");

        IndexRequest indexRequest = new IndexRequest();
        indexRequest.id("id");
        indexRequest.index("index");
        indexRequest.type("type");
        indexRequest.routing("routing");
        indexRequest.source(source);

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.id("id");
        updateRequest.index("index");
        updateRequest.type("type");
        updateRequest.routing("routing");
        updateRequest.doc(XContentFactory.jsonBuilder()
                .startObject()
                .field("url", "url")
                .endObject());
        updateRequest.upsert(indexRequest);

        bulkRequest.add(updateRequest);


        BulkResponse bulkResponse = bulkRequest.execute().get();
    }

}
