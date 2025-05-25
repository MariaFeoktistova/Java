package com.example.crawler.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Класс для управления подключением к Elasticsearch и вспомогательных функций.
 */
public class ElasticService implements Closeable {
    private final RestHighLevelClient client;
    private final String index;

    public ElasticService(String hostname, int port, String index) {
        this.client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(hostname, port, "http"))
        );
        this.index = index;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public String getIndex() {
        return index;
    }

    /**
     * Генерация уникального ID документа на основе title и pubDate с использованием SHA-256.
     */
    public String generateId(String title, String pubDate) {
        try {
            String input = title + "_" + pubDate;
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                hexString.append(String.format("%02x", b));
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to generate ID hash", e);
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
