package sqlancer.general;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

public class TestGeneralLearningScheduler {

    @Test
    public void testLearningIsThrottledPerScope() {
        AtomicLong now = new AtomicLong();
        GeneralLearningScheduler scheduler = new GeneralLearningScheduler(now::get);

        assertTrue(scheduler.tryAcquire("sqlite", 60));
        assertFalse(scheduler.tryAcquire("sqlite", 60));
        assertTrue(scheduler.tryAcquire("mysql", 60));

        now.set(TimeUnit.SECONDS.toNanos(59));
        assertFalse(scheduler.tryAcquire("sqlite", 60));
        now.set(TimeUnit.SECONDS.toNanos(60));
        assertTrue(scheduler.tryAcquire("sqlite", 60));
    }

    @Test
    public void testZeroIntervalDisablesThrottling() {
        GeneralLearningScheduler scheduler = new GeneralLearningScheduler(() -> 0L);

        assertTrue(scheduler.tryAcquire("sqlite", 0));
        assertTrue(scheduler.tryAcquire("sqlite", 0));
    }

    @Test
    public void testNegativeIntervalIsRejected() {
        GeneralLearningScheduler scheduler = new GeneralLearningScheduler(() -> 0L);

        assertThrows(IllegalArgumentException.class, () -> scheduler.tryAcquire("sqlite", -1));
    }
}
