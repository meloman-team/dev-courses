package tech.ydb.app;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Kirill Kurdyukov
 */
public record Issue(long id, String title, Instant now, String author, long linkCounts, String status) {
    public Issue(String title, String author) {
        this(ThreadLocalRandom.current().nextLong(), title, Instant.now(), author, 0, "");
    }
}