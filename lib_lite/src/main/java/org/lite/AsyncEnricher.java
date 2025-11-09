package org.lite;

import java.util.concurrent.CompletableFuture;

public interface AsyncEnricher<I, O> {
    CompletableFuture<O> apply(I in);
}
