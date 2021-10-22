package mongoflink.sink;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertManyResult;
import lombok.extern.slf4j.Slf4j;
import mongoflink.internal.connection.MongoClientProvider;
import org.apache.flink.api.connector.sink.Committer;
import org.bson.Document;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chenzhuoyu
 * @date 2021/9/17 22:13
 * MongoCommitter flushes data to MongoDB in a transaction. Due to MVCC implementation of MongoDB, a transaction is
 * not recommended to be large.
 **/
@Slf4j
public class MongoCommitter implements Committer<DocumentBulk> {

    private final MongoClient client;

    private final MongoCollection<Document> collection;


    private TransactionOptions txnOptions = TransactionOptions.builder()
            .readPreference(ReadPreference.primary())
            .readConcern(ReadConcern.LOCAL)
            .writeConcern(WriteConcern.MAJORITY)
            .build();

    public MongoCommitter(MongoClientProvider clientProvider) {
        this.client = clientProvider.getClient();
        this.collection = clientProvider.getDefaultCollection();
    }

    @Override
    public List<DocumentBulk> commit(List<DocumentBulk> committables) throws IOException {
        List<DocumentBulk> failedBulk = new ArrayList<>();
        for (DocumentBulk bulk : committables) {
            if (bulk.getDocuments().size() > 0) {
                log.info("|Flink操作|进行一次带有事务的Mongodb操作提交|");
//                CommittableTransaction transaction = new CommittableTransaction(collection, bulk.getDocuments());
                try (ClientSession session = client.startSession()){
                    session.startTransaction();
                    InsertManyResult insertManyResult = collection.insertMany(session, bulk.getDocuments());
                    if (!insertManyResult.wasAcknowledged()){
                        log.info("|Mongodb操作|进行写入失败进行事务回滚|");
                        session.abortTransaction();
                    }
                    session.commitTransaction();
                    //session.withTransaction(transaction, txnOptions);
                    log.info("|Mongodb操作|完成一次写入提交事务|");
                } catch (Exception e) {
                    // save to a new list that would be retried
                    log.error("Failed to commit with Mongo transaction", e);
                    failedBulk.add(bulk);
                }
            }
        }
        return failedBulk;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
