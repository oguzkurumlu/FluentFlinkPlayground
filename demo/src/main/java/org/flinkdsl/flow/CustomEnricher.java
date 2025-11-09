package org.flinkdsl.flow;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class CustomEnricher implements AsyncEnricher<NotificationCustomer, EnrichedCustomer> {
    private static final long serialVersionUID = 1L;

    private final Duration httpTimeout = Duration.ofSeconds(20);

    @Override
    public void asyncInvoke(NotificationCustomer notificationCustomer, ResultFuture<EnrichedCustomer> resultFuture) {
        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(httpTimeout)
                    .build();

            String url = "https://api.restful-api.dev/objects";

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(httpTimeout)
                    .GET()
                    .build();

            CompletableFuture<HttpResponse<String>> future =
                    client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                            .orTimeout(httpTimeout.toMillis(), TimeUnit.MILLISECONDS);

            future.whenComplete((response, ex) -> {
                if (ex != null) {
                    resultFuture.completeExceptionally(ex);
                    return;
                }

                try {
                    String body = response.body();
                    EnrichedCustomer enriched = new EnrichedCustomer();
                    enriched.enrichedValue = body;
                    enriched.fullName = notificationCustomer.fullName;
                    enriched.id = notificationCustomer.id;

                    resultFuture.complete(Collections.singleton(enriched));
                } catch (Exception e) {
                    resultFuture.completeExceptionally(e);
                }
            });
        } catch (Exception e) {
            resultFuture.completeExceptionally(e);
        }
    }
}
