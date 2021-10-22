package mongoflink;

import mongoflink.config.MongoOptions;
import mongoflink.sink.MongoSink;
import mongoflink.sink.MongoTransactionalSinkTest;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.util.Properties;
import java.util.Random;

/**
 * @author chenzhuoyu
 * @date 2021/10/20 13:27
 */
public class mongoTestCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(1000L);
        final Configuration config = new Configuration();
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();
        long rps = 50;
        long rows = 1000L;
        Properties properties = new Properties();
        properties.setProperty(MongoOptions.SINK_TRANSACTION_ENABLED, "false");
        properties.setProperty(MongoOptions.SINK_FLUSH_ON_CHECKPOINT, "false");
        properties.setProperty(MongoOptions.SINK_FLUSH_SIZE, String.valueOf(1_000L));
        properties.setProperty(MongoOptions.SINK_FLUSH_INTERVAL, String.valueOf(1_000L));
        env.addSource(new DataGeneratorSource<>(new StringGenerator(), rps, rows))
                .returns(String.class)
                .sinkTo(new MongoSink<>("mongodb://admin:SM67q89izW4itH7%25@192.168.220.180:27017/dev", "dev", "sda",
                        new StringDocumentSerializer(), properties));
        StreamGraph streamGraph = env.getStreamGraph(MongoTransactionalSinkTest.class.getName());
        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            miniCluster.executeJobBlocking(streamGraph.getJobGraph());
        }
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
            return count++ + "," + new Random().nextInt();
        }
    }
}
