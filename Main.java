package com.example.crawler;

import com.example.crawler.elastic.ArticleAggregator;
import com.example.crawler.elastic.ArticleIndexer;
import com.example.crawler.elastic.ArticleSearcher;
import com.example.crawler.elastic.ElasticService;

import java.util.HashMap;
import java.util.Map;

/**
 * Главный класс запуска приложения.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        ElasticService elastic = new ElasticService("localhost", 9200, "articles");
        ArticleIndexer indexer = new ArticleIndexer(elastic);
        ArticleSearcher searcher = new ArticleSearcher(elastic);
        ArticleAggregator aggregator = new ArticleAggregator(elastic);

        try {
            // Создаём пример статьи
            Map<String, Object> article = new HashMap<>();
            article.put("title", "AI conquers space");
            article.put("author", "Jane Doe");
            article.put("text", "AI-driven probes have landed.");
            article.put("pubDate", "2024-01-15");

            // Генерируем уникальный ID
            String id = elastic.generateId((String) article.get("title"), (String) article.get("pubDate"));
            // Индексируем статью
            indexer.index(article, id);

            // Выполняем различные поиски
            searcher.searchByTitle("space");
            searcher.searchByTitleAndAuthor("AI", "Jane Doe");
            searcher.searchByTitleOrAuthor("Mars", "John Doe");
            searcher.fuzzySearch("driveen");

            // Выполняем агрегации
            aggregator.aggregateByAuthor();
            aggregator.aggregateByDate();

        } finally {
            elastic.close();
        }
    }
}
