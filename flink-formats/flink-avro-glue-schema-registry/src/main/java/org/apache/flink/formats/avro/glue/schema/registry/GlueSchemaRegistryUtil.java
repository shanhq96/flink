package org.apache.flink.formats.avro.glue.schema.registry;

import java.util.Random;

public class GlueSchemaRegistryUtil {
    /**
     * Since glue schema registry service has TPS limit,
     * we would like to delay a while to reduce the peak TPS during flink application startup
     *
     * @param jitterBoundInMinutes
     */
    public static void injectJitter(final int jitterBoundInMinutes) {
        if (jitterBoundInMinutes != 0) {
            final Random random = new Random(System.currentTimeMillis());
            final int sleepSecond = random.nextInt(
                    jitterBoundInMinutes * 60
            );
            try {
                Thread.sleep(sleepSecond * 1000L);
            } catch (InterruptedException e) {
                // do nothing
            }
        }
    }
}
