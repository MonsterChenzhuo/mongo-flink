//package mongoflink.sink;
//
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.TransactionBody;
//import com.mongodb.client.model.UpdateOptions;
//import com.mongodb.client.model.Updates;
//import com.mongodb.client.result.InsertManyResult;
//import com.mongodb.client.result.UpdateResult;
//import lombok.extern.slf4j.Slf4j;
//import org.bson.BsonDocument;
//import org.bson.Document;
//import org.bson.conversions.Bson;
//
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.function.Consumer;
//
//import static com.mongodb.client.model.Filters.in;
//
///**
// * @author chenzhuoyu
// * @date 2021/9/17 22:13
// * An simple implementation of Mongo transaction body.
// **/
//
//public class CommittableTransaction implements TransactionBody<Long>, Serializable {
//
//    private final MongoCollection<Document> collection;
//
//    private List<Document> bufferedDocuments = new ArrayList<>(BUFFER_INIT_SIZE);
//
//    private static final int BUFFER_INIT_SIZE = 1024;
//
//    public CommittableTransaction(MongoCollection<Document> collection, List<Document> documents) {
//        this.collection = collection;
//        this.bufferedDocuments.addAll(documents);
//    }
//
//    @Override
//    public Long execute() {
//        //log.info("|Mongodb操作|进行一次带有事务的数据写入");
//        UpdateOptions options = new UpdateOptions().upsert(true);
//        List<String> keyList = new ArrayList<>();
//
//        bufferedDocuments.forEach(new Consumer<Document>() {
//            @Override
//            public void accept(Document document) {
//            keyList.add(String.valueOf(document.get("_id")));
//            }
//            });
//        Bson query = in("_id",keyList);
//        Bson updates = Updates.combine(bufferedDocuments);
////        BsonDocument bsonDocument = updates.toBsonDocument();
//        // ordered, non-bypass mode
//        UpdateResult updateResult = collection.updateMany(query, new Document("$set", updates), options);
//
//        return updateResult.getModifiedCount();
//    }
//
//
//
//}
