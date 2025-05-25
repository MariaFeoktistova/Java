package com.example.crawler.elastic;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;
import java.util.Map;

/**
 * Класс для индексации статей в Elasticsearch.
 */
public class ArticleIndexer {
    private final ElasticService service;

    public ArticleIndexer(ElasticService service) {
        this.service = service;
    }

    /**
     * Индексация статьи с заданным ID.
     */
    public void index(Map<String, Object> article, String id) throws IOException {
        IndexRequest request = new IndexRequest(service.getIndex())
                .id(id)
                .source(article);

        IndexResponse response = service.getClient().index(request, RequestOptions.DEFAULT);
        System.out.printf("Indexed document with ID %s, result: %s%n", id, response.getResult().name());
    }
}
