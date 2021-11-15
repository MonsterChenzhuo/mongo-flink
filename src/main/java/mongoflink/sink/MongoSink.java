package mongoflink.sink;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import mongoflink.serde.DocumentSerializer;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.mongodb.WriteConcern.MAJORITY;

/**
 * @author chenzhuoyu
 * @date 2021/9/17 22:13
 * <p> Flink sinkSQL connector for MongoDB. MongoSink supports transaction mode for MongoDB 4.2+ and non-transaction mode for
 * Mongo 3.0+. </p>
 *
 * <p> In transaction mode, all writes will be buffered in memory and committed to MongoDB in per-taskmanager
 * transactions on successful checkpoints, which ensures exactly-once semantics. </p>
 *
 * <p> In non-transaction mode, writes would be periodically flushed to MongoDB, which provides at-least-once semantics.
 * </p>
 **/
public class MongoSink<IN> implements Sink<IN, DocumentBulk, DocumentBulk, Void> {

    private DocumentSerializer<IN> serializer;
    private Boolean isTransactional;
    private MongoClient mongoClient;
    private MongoDatabase db;
    private MongoCollection<Document> collection;
    private long maxSize;
    private long bulkFlushInterval;
    private boolean flushOnCheckpoint;

    public static <IN> DefaultMongoSinkBuilder<IN>  BuilderClient(String connectionString,
                                                                 String database,
                                                                 String username,
                                                                 String collectionName,
                                                                 String password, DocumentSerializer<IN> serializer) {
        return new DefaultMongoSinkBuilder<IN>(connectionString, database, username, password, collectionName,serializer);
    }

    public static final class DefaultMongoSinkBuilder<IN>{
        private final String connectionString;
        private final String database;
        private final String username;
        private final String password;
        private final String collectionName;
        private DocumentSerializer<IN> serializer;
        private boolean isTransactional = Boolean.FALSE;
        private boolean retryWrites = true;
        private WriteConcern writeConcern = MAJORITY;
        private long timeout = -1L;
        private long maxSize = 1024L;
        private long bulkFlushInterval = 1000L;
        private boolean flushOnCheckpoint = true;

        public DefaultMongoSinkBuilder(String connectionString,
                                       String database,
                                       String username,
                                       String password,
                                       String collectionName,
                                       DocumentSerializer<IN> serializer) {
            this.connectionString = connectionString;
            this.database = database;
            this.username = username;
            this.password = password;
            this.serializer = serializer;
            this.collectionName = collectionName;
        }

        public DefaultMongoSinkBuilder<IN>  isTransactional(final Boolean isTransactional) {
            this.isTransactional = isTransactional;
            return this;
        }

        public DefaultMongoSinkBuilder<IN>  isRetryWrites(final Boolean retryWrites) {
            this.retryWrites = retryWrites;
            return this;
        }

        public DefaultMongoSinkBuilder<IN>  acknowledgmentOfWriteOperations(final WriteConcern writeConcern) {
            this.writeConcern = writeConcern;
            return this;
        }

        public DefaultMongoSinkBuilder<IN>  serverSelectionTimeout(final long timeout) {
            this.timeout = timeout;
            return this;
        }

        public DefaultMongoSinkBuilder<IN>  getBulkFlushInterval(final long bulkFlushInterval) {
            this.bulkFlushInterval = bulkFlushInterval;
            return this;
        }

        public DefaultMongoSinkBuilder<IN>  getMaxSize(final long maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public DefaultMongoSinkBuilder<IN>  isFlushOnCheckpoint(final Boolean flushOnCheckpoint) {
            this.flushOnCheckpoint = flushOnCheckpoint;
            return this;
        }

        //最后返回对象
        MongoSink build() {
            return new MongoSink(this.connectionString, this.database, this.username, this.password, this.collectionName, this.serializer
            ,this.isTransactional,this.retryWrites,this.writeConcern,this.timeout,this.maxSize,this.bulkFlushInterval,this.flushOnCheckpoint);
        }
    }


    //有参构造
    protected MongoSink(String connectionString,
                        String database,
                        String username,
                        String password,
                        String collectionName,
                        DocumentSerializer<IN> serializer,
                        Boolean isTransactional,
                        Boolean retryWrites,
                        WriteConcern writeConcern,
                        long timeout,
                        long maxSize,
                        long bulkFlushInterval,
                        boolean flushOnCheckpoint

    ) {
        this.serializer = serializer;
        this.isTransactional = isTransactional;
        this.maxSize = maxSize;
        this.bulkFlushInterval = bulkFlushInterval;
        this.flushOnCheckpoint = flushOnCheckpoint;


        MongoCredential credential = MongoCredential.createCredential(username, database, password.toCharArray());
        List<ServerAddress> serverList = new ArrayList();
        String[] serverAddressArr = connectionString.split(",");
        for (String serverAddressStr : serverAddressArr) {
            if (serverAddressStr.contains(":")) {
                serverList.add(new ServerAddress(serverAddressStr.split(":")[0],
                        Integer.parseInt(serverAddressStr.split(":")[1])));
            } else {
                serverList.add(new ServerAddress(serverAddressStr));
            }
        }
        MongoClientSettings settings = MongoClientSettings.builder()
                .credential(credential)
                .writeConcern(writeConcern)
                .retryWrites(retryWrites)
                .applyToClusterSettings(builder -> {
                    builder.hosts(serverList);
                    builder.serverSelectionTimeout(timeout, TimeUnit.SECONDS);
                }).build();
        mongoClient = MongoClients.create(settings);
        db = mongoClient.getDatabase(database);
        collection = db.getCollection(collectionName);

    }

    @Override
    public SinkWriter<IN, DocumentBulk, DocumentBulk> createWriter(InitContext initContext, List<DocumentBulk> states)
            throws IOException {
        MongoBulkWriter<IN> writer = new MongoBulkWriter<IN>(collection,serializer, maxSize,bulkFlushInterval,flushOnCheckpoint);
        writer.initializeState(states);
        return writer;
    }

    @Override
    public Optional<Committer<DocumentBulk>> createCommitter() throws IOException {
        if (isTransactional) {
            return Optional.of(new MongoCommitter(mongoClient,collection));
        }
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<DocumentBulk, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DocumentBulk>> getCommittableSerializer() {
        return Optional.of(DocumentBulkSerializer.INSTANCE);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DocumentBulk>> getWriterStateSerializer() {
        return Optional.of(DocumentBulkSerializer.INSTANCE);
    }
}
