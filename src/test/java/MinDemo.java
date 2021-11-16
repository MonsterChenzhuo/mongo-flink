import akka.stream.impl.io.FileSink;
import mongoflink.StringDocumentSerializer;
import mongoflink.config.MongoOptions;
import mongoflink.internal.connection.MongoClientProvider;
import mongoflink.internal.connection.MongoColloctionProviders;
import mongoflink.serde.DocumentDeserializer;
import mongoflink.sink.MongoSink;
import mongoflink.source.MongoSource;
import mongoflink.source.split.SamplingSplitStrategy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.bson.Document;
import org.junit.Test;

import java.util.Properties;
import java.util.Random;

import static com.mongodb.client.model.Filters.gte;


/**
 * @author chenzhuoyu
 * @date 2021/10/20 13:27
 */
public class MinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(1000L);


        long rps = 50;
        long rows = 1000L;

        MongoSink mongoSink = MongoSink.BuilderClient("admin","SM67q89izW4itH7%","192.168.221.201:27017,192.168.221.202:27018,192.168.221.202:27019",new StringDocumentSerializer())
                .isTransactional(false)
                .setDatabase("dev")
                .setCollection("sdsd")
                .isRetryWrites(true).build();

        env.addSource(new DataGeneratorSource<>(new StringGenerator(), rps, rows))
                .returns(String.class)
                .sinkTo(mongoSink);


        env.execute("cdc11c");



    }

    static class StringGenerator implements DataGenerator<String> {

        private int count;

        @Override
        public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public String next() {
            return "key" + count++ + "," + new Random().nextInt();
        }
    }

    @Test
    public void MongoSourceTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(1000L);

        MongoClientProvider clientProvider =  MongoColloctionProviders.getBuilder()
                .connectionString("mongodb://admin:SM67q89izW4itH7%25@192.168.220.180:27017/dev")
                .database("dev")
                .collection("sda").build();

        MongoSource<String> mongoSource = new MongoSource<>(
                clientProvider,
                (DocumentDeserializer<String>) Document::toJson,
                SamplingSplitStrategy.builder()
                        .setMatchQuery(gte("_id", "key48").toBsonDocument())
                        .setClientProvider(clientProvider)
                        .build()
        );

        env.fromSource(mongoSource,WatermarkStrategy.noWatermarks(),"mongo_batch_source")
                .returns(String.class).print("============>");

        env.execute("dsd");
    }
}
