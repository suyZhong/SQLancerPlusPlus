package sqlancer.general;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

final class GeneralLearningScheduler {

    private final LongSupplier nanoTime;
    private final Map<String, Long> lastLearningTime = new HashMap<>();

    GeneralLearningScheduler() {
        this(System::nanoTime);
    }

    GeneralLearningScheduler(LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
    }

    synchronized boolean tryAcquire(String scope, int intervalSeconds) {
        if (intervalSeconds < 0) {
            throw new IllegalArgumentException("The learning interval must not be negative");
        }

        long now = nanoTime.getAsLong();
        Long previous = lastLearningTime.get(scope);
        long intervalNanos = TimeUnit.SECONDS.toNanos(intervalSeconds);
        if (previous == null || intervalSeconds == 0 || now - previous >= intervalNanos) {
            lastLearningTime.put(scope, now);
            return true;
        }
        return false;
    }
}
