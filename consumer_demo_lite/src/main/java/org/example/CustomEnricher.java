package org.example;

import org.lite.AsyncEnricher;

import java.io.Serializable;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class CustomEnricher implements AsyncEnricher<NotificationCustomer, EnrichedCustomer>, Serializable {
    private static final long serialVersionUID = 1L;

    private static final String url = "https://api.restful-api.dev/objects";
    private static final Duration httpTimeout = Duration.ofSeconds(20);


    @Override
    public CompletableFuture<EnrichedCustomer> apply(NotificationCustomer in) {

        var client = HttpClient.newBuilder()
                .connectTimeout(httpTimeout)
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(httpTimeout)
                .GET()
                .build();

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .orTimeout(httpTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .thenApply(resp -> {
                    String body = resp.body();
                    EnrichedCustomer enriched = new EnrichedCustomer();
                    enriched.enrichedValue = body;
                    enriched.fullName = in.fullName;
                    enriched.id = in.id;
                    return enriched;
                });
    }
}