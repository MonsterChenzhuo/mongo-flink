package mongoflink.sink;

import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import lombok.extern.slf4j.Slf4j;
import mongoflink.config.SinkConfiguration;
import mongoflink.internal.connection.MongoClientProvider;
import mongoflink.serde.DocumentSerializer;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.Document;

import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Updates.set;

/**
 * @author chenzhuoyu
 * @date 2021/9/17 22:13
 * Writer for MongoDB sink.
 **/
@Slf4j
public class MongoBulkWriter<IN> implements SinkWriter<IN, DocumentBulk, DocumentBulk> {

    private final MongoClientProvider collectionProvider;

    private transient MongoCollection<Document> collection;

    private DocumentBulk currentBulk;

    private final List<DocumentBulk> pendingBulks = new ArrayList<>();

    private DocumentSerializer<IN> serializer;

    private transient ScheduledExecutorService scheduler;

    private transient ScheduledFuture scheduledFuture;

    private transient volatile Exception flushException;

    private final long maxSize;

    private final boolean flushOnCheckpoint;

    private final RetryPolicy retryPolicy = new RetryPolicy(3, 1000L);

    private transient volatile boolean closed = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoBulkWriter.class);

    public MongoBulkWriter(MongoClientProvider collectionProvider,
                           DocumentSerializer<IN> serializer,
                           SinkConfiguration configuration) {
        this.collectionProvider = collectionProvider;
        this.serializer = serializer;
        this.maxSize = configuration.getBulkFlushSize();
        this.currentBulk = new DocumentBulk(maxSize);
        this.flushOnCheckpoint = configuration.isFlushOnCheckpoint();
        if (!flushOnCheckpoint && configuration.getBulkFlushInterval() > 0) {
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            1, new ExecutorThreadFactory("mongodb-bulk-writer"));
            this.scheduledFuture =
                    scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (MongoBulkWriter.this) {
                                    if (!closed) {
                                        try {
                                            rollBulkIfNeeded(true);
                                            flush();
                                            log.info("|Mongdb操作|完成一次Mongo非事务flush操作|");
                                        } catch (Exception e) {
                                            log.error("|Mongodb操作|Mongo非事务flush操作异常|");
                                            flushException = e;
                                        }
                                    }
                                }
                            },
                            configuration.getBulkFlushInterval(),
                            configuration.getBulkFlushInterval(),
                            TimeUnit.MILLISECONDS);
        }
    }

    public void initializeState(List<DocumentBulk> recoveredBulks) {
        collection = collectionProvider.getDefaultCollection();
        for (DocumentBulk bulk: recoveredBulks) {
            for (Document document: bulk.getDocuments()) {
                rollBulkIfNeeded();
                currentBulk.add(document);
            }
        }
    }

    @Override
    public void write(IN o, Context context) throws IOException {
        checkFlushException();
        rollBulkIfNeeded();
        log.info("|Flink操作|将数据流缓存至桶中");
        currentBulk.add(serializer.serialize(o));
    }

    @Override
    public List<DocumentBulk> prepareCommit(boolean flush) throws IOException {
        if (flushOnCheckpoint || flush) {
            rollBulkIfNeeded(true);
        }
        return pendingBulks;
    }

    @Override
    public List<DocumentBulk> snapshotState() throws IOException {
        List<DocumentBulk> inProgressAndPendingBulks = new ArrayList<>(1);
        inProgressAndPendingBulks.add(currentBulk);
        inProgressAndPendingBulks.addAll(pendingBulks);
        pendingBulks.clear();
        log.info("|Flink操作|进行一次数据快照状态上报|");
        return inProgressAndPendingBulks;
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    /**
     * Flush by non-transactional bulk write, which may result in data duplicates after multiple tries.
     * There may be concurrent flushes when concurrent checkpoints are enabled.
     *
     * We manually retry write operations, because the driver doesn't support automatic retries for some MongoDB
     * setups (e.g. standalone instances). TODO: This should be configurable in the future.
     */
    private synchronized void flush() {
        if (!closed) {
            ensureConnection();
            retryPolicy.reset();
            Iterator<DocumentBulk> iterator = pendingBulks.iterator();
            while (iterator.hasNext()) {
                DocumentBulk bulk = iterator.next();
                do {
                    try {
                        //UpdateOptions options = new UpdateOptions().upsert(true);
                        List<Document> documents = bulk.getDocuments();
                        if (documents.size()==0){
                            log.info("|Mongdb操作|Mongo非事务flush操作|待刷写数据集为空跳出此次刷写|");
                            break;
                        }

                        List<WriteModel<Document>> batchOperateList = new ArrayList<>();
                        documents.forEach(new Consumer<Document>() {
                            @Override
                            public void accept(Document document) {
                                Bson filter = Filters.eq("_id", document.get("_id"));
                                UpdateOptions options = new UpdateOptions().upsert(true);
                                UpdateOneModel<Document> updateOneModel = new UpdateOneModel<>(filter, new Document("$set", document),options);
                                batchOperateList.add(updateOneModel);
                            }
                        });
                        BulkWriteOptions options = new BulkWriteOptions();
                        // 关闭排序后，mongo会对writeModelList操作重新排序，提高效率
                        options.ordered(false);
                        BulkWriteResult bulkWriteResult = collection.bulkWrite(batchOperateList, options);
                        log.info("【mongo数据处理】BulkWrite操作是否成功="+bulkWriteResult.wasAcknowledged());
                        iterator.remove();
                        break;
                    } catch (MongoException e) {
                        // maybe partial failure
                        log.error("|Mongdb操作|Mongo非事务flush操作异常|Failed to flush data to MongoDB", e);
                    }
                } while (!closed && retryPolicy.shouldBackoffRetry());
            }
        }
    }

    private void ensureConnection() {
        try {
            collection.listIndexes();
        } catch (MongoException e) {
            LOGGER.warn("|Mongdb操作|Connection is not available, try to reconnect", e);
            collectionProvider.recreateClient();
        }
    }

    private void rollBulkIfNeeded() {
        rollBulkIfNeeded(false);
    }

    private synchronized void rollBulkIfNeeded(boolean force) {
        if (force || currentBulk.isFull()) {
            pendingBulks.add(currentBulk);
            currentBulk = new DocumentBulk(maxSize);
        }
    }

    private void checkFlushException() throws IOException {
        if (flushException != null) {
            throw new IOException("Failed to flush records to MongoDB", flushException);
        }
    }

    @NotThreadSafe
    class RetryPolicy {

        private final long maxRetries;

        private final long backoffMillis;

        private long currentRetries = 0L;

        RetryPolicy(long maxRetries, long backoffMillis) {
            this.maxRetries = maxRetries;
            this.backoffMillis = backoffMillis;
        }

        boolean shouldBackoffRetry() {
            if (++currentRetries > maxRetries) {
                return false;
            } else {
                backoff();
                return true;
            }
        }

        private void backoff() {
            try {
                Thread.sleep(backoffMillis);
            } catch (InterruptedException e) {
                // exit backoff
            }
        }

        void reset() {
            currentRetries = 0L;
        }
    }
}
