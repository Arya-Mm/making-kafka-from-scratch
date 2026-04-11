package com.simplekafka.frontend;

import com.simplekafka.client.SimpleKafkaClient;
import com.simplekafka.client.SimpleKafkaProducer;
import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Protocol;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleKafkaWebServer {

    private static final String INDEX_RESOURCE = "/web/index.html";

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        new SimpleKafkaWebServer().start(port);
    }

    public void start(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", this::handleRoot);
        server.createContext("/api/create-topic", this::handleCreateTopic);
        server.createContext("/api/produce", this::handleProduce);
        server.createContext("/api/fetch", this::handleFetch);
        server.createContext("/api/metadata", this::handleMetadata);
        server.setExecutor(null);
        server.start();
        System.out.println("Web UI started at http://localhost:" + port);
    }

    private void addCorsHeaders(HttpExchange exchange) {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");
    }

    private void handleRoot(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendJsonResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
            return;
        }

        byte[] content = readResource(INDEX_RESOURCE);
        if (content == null) {
            sendJsonResponse(exchange, 500, "{\"error\":\"Index page not found\"}");
            return;
        }

        exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
        addCorsHeaders(exchange);
        exchange.sendResponseHeaders(200, content.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(content);
        }
    }

    private void handleOptions(HttpExchange exchange) throws IOException {
        addCorsHeaders(exchange);
        exchange.sendResponseHeaders(204, -1);
    }

    private void handleCreateTopic(HttpExchange exchange) throws IOException {
        if ("OPTIONS".equals(exchange.getRequestMethod())) {
            handleOptions(exchange);
            return;
        }
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendJsonResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
            return;
        }

        Map<String, String> params = readRequestParameters(exchange);
        String host = params.getOrDefault("host", "localhost");
        int port = parseInt(params.get("port"), 9092);
        String topic = params.get("topic");
        int partitions = parseInt(params.get("partitions"), 1);
        int replication = parseInt(params.get("replication"), 1);

        if (topic == null || topic.isBlank()) {
            sendJsonResponse(exchange, 400, "{\"error\":\"Topic is required\"}");
            return;
        }

        try {
            SimpleKafkaClient client = new SimpleKafkaClient(host, port);
            client.initialize();
            client.createTopic(topic, partitions, (short) replication);
            sendJsonResponse(exchange, 200, "{\"success\":true,\"message\":\"Topic created\"}");
        } catch (Exception e) {
            sendJsonResponse(exchange, 500, "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
        }
    }

    private void handleProduce(HttpExchange exchange) throws IOException {
        if ("OPTIONS".equals(exchange.getRequestMethod())) {
            handleOptions(exchange);
            return;
        }
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendJsonResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
            return;
        }

        Map<String, String> params = readRequestParameters(exchange);
        String host = params.getOrDefault("host", "localhost");
        int port = parseInt(params.get("port"), 9092);
        String topic = params.get("topic");
        String message = params.get("message");
        int partition = params.containsKey("partition") ? parseInt(params.get("partition"), -1) : -1;

        if (topic == null || topic.isBlank()) {
            sendJsonResponse(exchange, 400, "{\"error\":\"Topic is required\"}");
            return;
        }
        if (message == null) {
            sendJsonResponse(exchange, 400, "{\"error\":\"Message is required\"}");
            return;
        }

        try {
            SimpleKafkaProducer producer = new SimpleKafkaProducer(host, port, topic);
            producer.initialize();
            long offset;
            if (partition >= 0) {
                offset = producer.send(message, partition);
            } else {
                offset = producer.send(message);
            }
            sendJsonResponse(exchange, 200, "{\"success\":true,\"offset\":" + offset + "}");
        } catch (Exception e) {
            sendJsonResponse(exchange, 500, "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
        }
    }

    private void handleFetch(HttpExchange exchange) throws IOException {
        if ("OPTIONS".equals(exchange.getRequestMethod())) {
            handleOptions(exchange);
            return;
        }
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendJsonResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
            return;
        }

        Map<String, String> params = parseQuery(exchange.getRequestURI().getRawQuery());
        String host = params.getOrDefault("host", "localhost");
        int port = parseInt(params.get("port"), 9092);
        String topic = params.get("topic");
        int partition = parseInt(params.get("partition"), 0);
        long offset = parseLong(params.get("offset"), 0L);
        int maxMessages = parseInt(params.get("maxMessages"), 10);

        if (topic == null || topic.isBlank()) {
            sendJsonResponse(exchange, 400, "{\"error\":\"Topic is required\"}");
            return;
        }

        try {
            SimpleKafkaClient client = new SimpleKafkaClient(host, port);
            client.initialize();

            var records = client.fetchWithOffsets(topic, partition, offset, maxMessages);

            StringBuilder builder = new StringBuilder();
            builder.append("{\"success\":true,\"messages\":[");
            for (int i = 0; i < records.size(); i++) {
                var record = records.get(i);
                String text = new String(record.getMessage(), StandardCharsets.UTF_8);
                builder.append("{")
                        .append("\"offset\":").append(record.getOffset()).append(",")
                        .append("\"message\":\"").append(escapeJson(text)).append("\"")
                        .append("}");
                if (i < records.size() - 1) {
                    builder.append(",");
                }
            }
            builder.append("]}");

            sendJsonResponse(exchange, 200, builder.toString());
        } catch (Exception e) {
            sendJsonResponse(exchange, 500, "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
        }
    }

    private void handleMetadata(HttpExchange exchange) throws IOException {
        if ("OPTIONS".equals(exchange.getRequestMethod())) {
            handleOptions(exchange);
            return;
        }
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendJsonResponse(exchange, 405, "{\"error\":\"Method not allowed\"}");
            return;
        }

        Map<String, String> params = parseQuery(exchange.getRequestURI().getRawQuery());
        String host = params.getOrDefault("host", "localhost");
        int port = parseInt(params.get("port"), 9092);

        try {
            SimpleKafkaClient client = new SimpleKafkaClient(host, port);
            client.initialize();

            Map<String, SimpleKafkaClient.TopicMetadata> topicMetadata = client.topicMetadata();
            List<BrokerInfo> brokerList = client.brokers();

            StringBuilder builder = new StringBuilder();
            builder.append("{\"success\":true,\"brokers\":[");
            for (int i = 0; i < brokerList.size(); i++) {
                BrokerInfo broker = brokerList.get(i);
                builder.append("{")
                        .append("\"id\":").append(broker.getId()).append(",")
                        .append("\"host\":\"").append(escapeJson(broker.getHost())).append("\",")
                        .append("\"port\":").append(broker.getPort())
                        .append("}");
                if (i < brokerList.size() - 1) {
                    builder.append(",");
                }
            }
            builder.append("],\"topics\":[");
            int topicCount = 0;
            for (var entry : topicMetadata.entrySet()) {
                if (topicCount > 0) {
                    builder.append(",");
                }
                builder.append("{")
                        .append("\"topic\":\"").append(escapeJson(entry.getKey())).append("\",")
                        .append("\"partitions\":").append(entry.getValue().partitions().size())
                        .append("}");
                topicCount++;
            }
            builder.append("]}");

            sendJsonResponse(exchange, 200, builder.toString());
        } catch (Exception e) {
            sendJsonResponse(exchange, 500, "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
        }
    }

    private Map<String, String> readRequestParameters(HttpExchange exchange) throws IOException {
        String rawBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        if (rawBody != null && rawBody.contains("{")) {
            return parseJson(rawBody);
        }
        return parseQuery(rawBody);
    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> result = new HashMap<>();
        if (query == null || query.isBlank()) {
            return result;
        }
        for (String pair : query.split("&")) {
            int idx = pair.indexOf('=');
            if (idx > 0) {
                String key = decodeUrl(pair.substring(0, idx));
                String value = decodeUrl(pair.substring(idx + 1));
                result.put(key, value);
            }
        }
        return result;
    }

    private Map<String, String> parseJson(String json) {
        Map<String, String> result = new HashMap<>();
        if (json == null || json.isBlank()) {
            return result;
        }
        json = json.trim();
        if (json.startsWith("{") && json.endsWith("}")) {
            json = json.substring(1, json.length() - 1);
        }
        int index = 0;
        while (index < json.length()) {
            int quoteStart = json.indexOf('"', index);
            if (quoteStart < 0) break;
            int quoteEnd = json.indexOf('"', quoteStart + 1);
            if (quoteEnd < 0) break;
            String key = json.substring(quoteStart + 1, quoteEnd);
            int colon = json.indexOf(':', quoteEnd);
            if (colon < 0) break;
            index = colon + 1;
            while (index < json.length() && Character.isWhitespace(json.charAt(index))) {
                index++;
            }
            String value;
            if (index < json.length() && json.charAt(index) == '"') {
                int valueStart = index + 1;
                int valueEnd = json.indexOf('"', valueStart);
                while (valueEnd > 0 && json.charAt(valueEnd - 1) == '\\') {
                    valueEnd = json.indexOf('"', valueEnd + 1);
                }
                if (valueEnd < 0) break;
                value = json.substring(valueStart, valueEnd).replace("\\\"", "\"").replace("\\\\", "\\");
                index = valueEnd + 1;
            } else {
                int valueEnd = json.indexOf(',', index);
                if (valueEnd < 0) {
                    valueEnd = json.length();
                }
                value = json.substring(index, valueEnd).trim();
                index = valueEnd;
            }
            result.put(key, value);
            if (index < json.length() && json.charAt(index) == ',') {
                index++;
            }
        }
        return result;
    }

    private String decodeUrl(String value) {
        return java.net.URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private byte[] readResource(String resourcePath) throws IOException {
        try (InputStream in = getClass().getResourceAsStream(resourcePath)) {
            if (in == null) {
                return null;
            }
            return in.readAllBytes();
        }
    }

    private void sendJsonResponse(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
        addCorsHeaders(exchange);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private int parseInt(String value, int defaultValue) {
        try {
            return value == null ? defaultValue : Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private long parseLong(String value, long defaultValue) {
        try {
            return value == null ? defaultValue : Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private String escapeJson(String text) {
        if (text == null) {
            return "";
        }
        return text
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }
}