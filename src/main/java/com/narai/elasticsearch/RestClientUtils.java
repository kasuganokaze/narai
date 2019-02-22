package com.narai.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * @author: kaze
 * @date: 2019-02-22
 */
@Slf4j
@Component
public class RestClientUtils {

    private final static String POST = "POST";
    private final static String SEARCH = "_search";
    private final static String UPDATE = "_update_by_query";
    private final static String DELETE = "_delete_by_query";
    private static RestClient restClient;

    @Resource
    public void setRestClient(RestClient restClient) {
        RestClientUtils.restClient = restClient;
    }

    /**
     * 新增记录
     */
    public static String doAdd(String index, String jsonParam) {
        String endpoint = index + "/" + index + "/" + UUID.randomUUID().toString().replaceAll("-", "") + "?refresh=true";
        try {
            return renderForEs(endpoint, jsonParam, POST);
        } catch (IOException e) {
            log.error("", e);
        }
        return null;
    }

    /**
     * 删除记录
     */
    public static String doDelete(String index, Map<String, Object> query) {
        return doDelete(Collections.singletonList(index), query);
    }

    /**
     * 删除记录
     */
    public static String doDelete(Collection<String> indexes, Map<String, Object> query) {
        String index = indexes.parallelStream().reduce((a, b) -> a + "," + b).get();
        String endpoint = index + "/" + DELETE + "?refresh=true";
        String queryResult = query.entrySet().stream()
                .map(a -> "{\"term\":{\"" + a.getKey() + "\":{\"value\": \"" + a.getValue() + "\"}}}")
                .reduce((a, b) -> a + "," + b).get();
        try {
            String jsonParam = "{\n" +
                    "  \"query\": {\n" +
                    "    \"bool\": {\n" +
                    "      \"must\": [\n" + queryResult +
                    "      ]\n" +
                    "    }\n" +
                    "  }\n" +
                    "}";
            return renderForEs(endpoint, jsonParam, POST);
        } catch (IOException e) {
            log.error("", e);
        }
        return null;
    }

    /**
     * 更新记录
     */
    public static String doUpdate(String index, Map<String, Object> query, Map<String, Object> param) {
        return doUpdate(Collections.singletonList(index), query, param);
    }

    /**
     * 更新记录
     */
    public static String doUpdate(String index, String jsonParam) {
        return doUpdate(Collections.singletonList(index), jsonParam);
    }

    /**
     * 更新记录
     */
    public static String doUpdate(Collection<String> indexes, Map<String, Object> query, Map<String, Object> param) {
        String index = indexes.parallelStream().reduce((a, b) -> a + "," + b).get();
        String endpoint = index + "/" + UPDATE + "?refresh=true";
        String queryResult = query.entrySet().stream()
                .map(a -> "{\"term\":{\"" + a.getKey() + "\":{\"value\": \"" + a.getValue() + "\"}}}")
                .reduce((a, b) -> a + "," + b).get();
        String paramResult = param.entrySet().stream()
                .map(a -> "ctx._source['" + a.getKey() + "']='" + a.getValue() + "';")
                .reduce((a, b) -> a + b).get();
        try {
            String jsonParam = "{\n" +
                    "  \"query\": {\n" +
                    "    \"bool\": {\n" +
                    "      \"must\": [\n" + queryResult +
                    "      ]\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"script\": \"" + paramResult + "\"\n" +
                    "}";
            return renderForEs(endpoint, jsonParam, POST);
        } catch (IOException e) {
            log.error("", e);
        }
        return null;
    }

    /**
     * 更新记录
     */
    public static String doUpdate(Collection<String> indexes, String jsonParam) {
        String index = indexes.parallelStream().reduce((a, b) -> a + "," + b).get();
        String endpoint = index + "/" + UPDATE + "?refresh=true";
        try {
            return renderForEs(endpoint, jsonParam, POST);
        } catch (IOException e) {
            log.error("", e);
        }
        return null;
    }

    /**
     * 查询记录
     */
    public static String doSearch(String index, String jsonParam) {
        return doSearch(Collections.singletonList(index), jsonParam);
    }

    /**
     * 查询记录
     */
    public static String doSearch(Collection<String> indexes, String jsonParam) {
        String index = indexes.parallelStream().reduce((a, b) -> a + "," + b).get();
        String endpoint = index + "/" + SEARCH;
        try {
            return renderForEs(endpoint, jsonParam, POST);
        } catch (IOException e) {
            log.error("", e);
        }
        return null;
    }

    private static String renderForEs(String endpoint, String jsonParam, String method) throws IOException {
        Map<String, String> params = Collections.emptyMap();
        HttpEntity entity = new NStringEntity(jsonParam, ContentType.APPLICATION_JSON);
        Response response = restClient.performRequest(method, endpoint, params, entity);
        return EntityUtils.toString(response.getEntity());
    }

}
