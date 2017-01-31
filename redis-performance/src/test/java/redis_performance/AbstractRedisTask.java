package redis_performance;

import io.vertx.core.Vertx;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractRedisTask implements VertexLoopUtils.AsyncTask {
    final AtomicInteger connections = new AtomicInteger();
    final AtomicLong timeSpentOpeningConnection = new AtomicLong();
    private final ThreadLocal<RedisClient> clientThreadLocal = new ThreadLocal<>();
    private final Vertx vertx;
    private final boolean keepConnectionAlive;
    private final String redisHost;
    private final int redisPort;

    public AbstractRedisTask(final Vertx vertx, final boolean keepConnectionAlive, final String redisHost, final int redisPort) {
        this.vertx = vertx;
        this.keepConnectionAlive = keepConnectionAlive;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public void work(final VertexLoopUtils.OnTaskCompleted onTaskCompleted) {
        if (!keepConnectionAlive) {
            final long startOpeningConn = System.currentTimeMillis();
            final RedisClient client = RedisClient.create(vertx, new RedisOptions().setHost(redisHost).setPort(redisPort));
            timeSpentOpeningConnection.addAndGet(System.currentTimeMillis() - startOpeningConn);
            work(client, () -> {
                final long startClosingConn = System.currentTimeMillis();
                client.close((r) -> {
                    connections.incrementAndGet();
                    timeSpentOpeningConnection.addAndGet(System.currentTimeMillis() - startClosingConn);
                    if (r.failed()) {
                        System.out.println("Error closing redis connection: " + r.cause());
                    }
                    onTaskCompleted.completed();
                });
            });
        } else {
            RedisClient _client = clientThreadLocal.get();
            if (_client == null) {
                _client = RedisClient.create(vertx, new RedisOptions().setHost(redisHost).setPort(redisPort));
                clientThreadLocal.set(_client);
                connections.incrementAndGet();
            }
            final RedisClient client = _client;
            work(client, onTaskCompleted);
        }
    }

    abstract void work(final RedisClient client, final VertexLoopUtils.OnTaskCompleted onTaskCompleted);
}
