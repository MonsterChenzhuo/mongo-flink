package mongoflink.sinkSQL.sink;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import mongoflink.internal.connection.MongoClientProvider;
import mongoflink.internal.connection.MongoColloctionProviders;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenzhuoyu
 * @date 2021/9/17 22:13
 *  建立连接，插入数据
 */
public abstract class MongodbBaseSinkFunction<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
    private final MongodbSinkConf mongodbSinkConf;
    private transient MongoClient client;
    private transient List<Document> batch;
    private final MongoClientProvider clientProvider;

    protected MongodbBaseSinkFunction(MongodbSinkConf mongodbSinkConf) {
        this.mongodbSinkConf = mongodbSinkConf;
        this.clientProvider = MongoColloctionProviders
                .getBuilder()
                .connectionString(this.mongodbSinkConf.getUri())
                .database(this.mongodbSinkConf.getDatabase())
                .collection(this.mongodbSinkConf.getCollection()).build();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.client = clientProvider.getClient();
        this.batch = new ArrayList();
    }



    @Override
    public void close() throws Exception {
        flush();
        super.close();
        this.client.close();
        this.client = null;
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        this.batch.add(invokeDocument(value, context));
        if (this.batch.size() >= this.mongodbSinkConf.getBatchSize()) {
            flush();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) {
    }

    private void flush() {
        if (this.batch.isEmpty()) {
            return;
        }
        MongoDatabase mongoDatabase = this.client.getDatabase(this.mongodbSinkConf.getDatabase());

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(this.mongodbSinkConf.getCollection());
        mongoCollection.insertMany(this.batch);

        this.batch.clear();
    }

    abstract Document invokeDocument(IN paramIN, Context paramContext) throws Exception;
}
