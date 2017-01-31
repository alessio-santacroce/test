package redis_performance;


import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.redis.RedisClient;

import java.util.concurrent.atomic.AtomicInteger;

public class TestConnDisconn {
    public static void main(final String[] args) {
        final Vertx vertx = Vertx.vertx(new VertxOptions()
                        .setMaxEventLoopExecuteTime(60000)
                        .setMaxWorkerExecuteTime(60000)
//                .setWarningExceptionTime(60000)
        );

        final String redisHost = "localhost";
        final int redisPort = 6379;
        final boolean keepConnectionAlive = false;
        final long delay = 0;
        final int repetitions = 8000000;
        final int concurrency = 50;

        final AbstractRedisTask redisTask = new AbstractRedisTask(vertx, keepConnectionAlive, redisHost, redisPort) {
            final AtomicInteger atomicInteger = new AtomicInteger();

            @Override
            void work(final RedisClient client, final VertexLoopUtils.OnTaskCompleted onTaskCompleted) {
                final int id = atomicInteger.incrementAndGet();
                if (id % 1000 == 0) {
                    System.out.println("\nconn: " + id);
                }
                onTaskCompleted.completed();
            }
        };

        final long startedAt = System.currentTimeMillis();
        VertexLoopUtils.loop(vertx, repetitions, concurrency, redisTask,
                () -> {
                    final long endedAt = System.currentTimeMillis();
                    final long timeSec = (endedAt - startedAt) / 1000;
                    final long ratePerSec = repetitions / timeSec;
                    System.out.println("** TEST ==> " + TestConnDisconn.class.getSimpleName());
                    System.out.println("redis " + redisHost + ':' + redisPort);
                    System.out.println("keepConnectionAlive: " + keepConnectionAlive);
                    System.out.println("delay: " + delay);
                    System.out.println("items added in zset: " + repetitions);
                    System.out.println("concurrency: " + concurrency);
                    System.out.println("average time to open connection millisec: "
                            + (redisTask.timeSpentOpeningConnection.get() / redisTask.connections.get()));
                    System.out.println("tot time sec: " + timeSec);
                    System.out.println("ratePerSec: " + ratePerSec);
                    vertx.close();
                });
    }
}
