package com.narai.elasticsearch.util;

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

    private final static String DELETE = "_delete_by_query";
    private final static String UPDATE = "_update_by_query";
    private final static String SEARCH = "_search";
    private static RestClient restClient;

    @Resource
    public void setRestClient(RestClient restClient) {
        RestClientUtils.restClient = restClient;
    }

    /**
     * 新增记录
     */
    public static void insert(String index, String type, String jsonParam) {
        String id = UUID.randomUUID().toString().replaceAll("-", "");
        String endpoint = index + "/" + type + "/" + id + "?refresh=true";
        try {
            renderForEs(endpoint, jsonParam);
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 删除记录
     */
    public static String delete(String index, String type, Map<String, Object> query) {
        String endpoint = index + "/" + type + "/" + DELETE + "?refresh=true";
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
            return renderForEs(endpoint, jsonParam);
        } catch (IOException e) {
            log.error("", e);
        }
        return null;
    }

    /**
     * 更新记录
     */
    public static String update(String index, String type, Map<String, Object> query, Map<String, Object> param) {
        String endpoint = index + "/" + type + "/" + UPDATE + "?refresh=true";
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
            return renderForEs(endpoint, jsonParam);
        } catch (IOException e) {
            log.error("", e);
        }
        return null;
    }

    /**
     * 查询记录
     */
    public static String search(String index, String type, Map<String, Object> query) {
        String endpoint = index + "/" + type + "/" + SEARCH;
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
            return renderForEs(endpoint, jsonParam);
        } catch (IOException e) {
            log.error("", e);
        }
        return null;
    }

    /**
     * 查询记录
     */
    public static String search(String index, String type, String jsonParam) {
        String endpoint = index + "/" + type + "/" + SEARCH;
        try {
            return renderForEs(endpoint, jsonParam);
        } catch (IOException e) {
            log.error("", e);
        }
        return null;
    }

    private static String renderForEs(String endpoint, String jsonParam) throws IOException {
        Map<String, String> params = Collections.emptyMap();
        HttpEntity entity = new NStringEntity(jsonParam, ContentType.APPLICATION_JSON);
        Response response = restClient.performRequest("POST", endpoint, params, entity);
        return EntityUtils.toString(response.getEntity());
    }

}
