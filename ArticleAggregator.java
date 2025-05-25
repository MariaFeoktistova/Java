package com.example.crawler.elastic;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * Класс для выполнения агрегаций в Elasticsearch.
 */
public class ArticleAggregator {
    private final ElasticService service;

    public ArticleAggregator(ElasticService service) {
        this.service = service;
    }

    /**
     * Агрегация: подсчёт количества статей по авторам.
     */
    public void aggregateByAuthor() throws IOException {
        SearchRequest request = new SearchRequest(service.getIndex());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.size(0); // не возвращаем документы, только агрегации
        sourceBuilder.aggregation(
                AggregationBuilders.terms("authors").field("author.keyword")
        );

        request.source(sourceBuilder);
        SearchResponse response = service.getClient().search(request, RequestOptions.DEFAULT);

        Terms authors = response.getAggregations().get("authors");
        System.out.println("[Aggregation] Articles by author:");
        for (Terms.Bucket bucket : authors.getBuckets()) {
            System.out.printf("Author: %s, Articles: %d%n", bucket.getKeyAsString(), bucket.getDocCount());
        }
    }

    /**
     * Агрегация: подсчёт количества статей по датам публикации.
     */
    public void aggregateByDate() throws IOException {
        SearchRequest request = new SearchRequest(service.getIndex());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.size(0);
        sourceBuilder.aggregation(
                AggregationBuilders.terms("pubDates").field("pubDate.keyword")
        );

        request.source(sourceBuilder);
        SearchResponse response = service.getClient().search(request, RequestOptions.DEFAULT);

        Terms dates = response.getAggregations().get("pubDates");
        System.out.println("[Aggregation] Articles by publication date:");
        for (Terms.Bucket bucket : dates.getBuckets()) {
            System.out.printf("Date: %s, Articles: %d%n", bucket.getKeyAsString(), bucket.getDocCount());
        }
    }
}
