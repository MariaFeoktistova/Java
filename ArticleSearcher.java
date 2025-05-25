package com.example.crawler.elastic;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * Класс для выполнения поисковых запросов в Elasticsearch.
 */
public class ArticleSearcher {
    private final ElasticService service;

    public ArticleSearcher(ElasticService service) {
        this.service = service;
    }

    public void searchByTitle(String title) throws IOException {
        var query = QueryBuilders.matchQuery("title", title);
        search(query, "[Search] By title");
    }

    public void searchByTitleAndAuthor(String title, String author) throws IOException {
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("title", title))
                .must(QueryBuilders.matchQuery("author", author));
        search(query, "[Search] By title AND author");
    }

    public void searchByTitleOrAuthor(String title, String author) throws IOException {
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .should(QueryBuilders.matchQuery("title", title))
                .should(QueryBuilders.matchQuery("author", author))
                .minimumShouldMatch(1);
        search(query, "[Search] By title OR author");
    }

    public void fuzzySearch(String text) throws IOException {
        var query = QueryBuilders.matchQuery("text", text).fuzziness("AUTO");
        search(query, "[Search] Fuzzy match");
    }

    private void search(org.elasticsearch.index.query.QueryBuilder query, String label) throws IOException {
        SearchRequest request = new SearchRequest(service.getIndex());
        SearchSourceBuilder builder = new SearchSourceBuilder().query(query);
        request.source(builder);

        SearchResponse response = service.getClient().search(request, RequestOptions.DEFAULT);
        System.out.printf("%s: %d hits%n", label, response.getHits().getTotalHits().value);
    }
}
