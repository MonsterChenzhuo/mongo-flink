//import akka.stream.impl.io.FileSink;
//import mongoflink.StringDocumentSerializer;
//import mongoflink.config.MongoOptions;
//import mongoflink.sink.MongoSink;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.configuration.RestOptions;
//import org.apache.flink.runtime.minicluster.MiniCluster;
//import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
//import org.apache.flink.runtime.state.FunctionInitializationContext;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
//import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
//import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
//import org.apache.flink.streaming.api.graph.StreamGraph;
//
//import java.util.Properties;
//import java.util.Random;
//
///**
// * @author chenzhuoyu
// * @date 2021/10/20 13:27
// */
//public class MinDemo {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.getCheckpointConfig().setCheckpointInterval(1000L);
//        final Configuration config = new Configuration();
//        config.setString(RestOptions.BIND_PORT, "18081-19000");
//        final MiniClusterConfiguration cfg =
//                new MiniClusterConfiguration.Builder()
//                        .setNumTaskManagers(1)
//                        .setNumSlotsPerTaskManager(4)
//                        .setConfiguration(config)
//                        .build();
//        Properties properties = new Properties();
//        properties.setProperty(MongoOptions.SINK_TRANSACTION_ENABLED, "false");
//        properties.setProperty(MongoOptions.SINK_FLUSH_ON_CHECKPOINT, "false");
//        properties.setProperty(MongoOptions.SINK_FLUSH_SIZE, String.valueOf(5L));
//        properties.setProperty(MongoOptions.SINK_FLUSH_INTERVAL, String.valueOf(10_000L));
//        long rps = 50;
//        long rows = 1000L;
//        env.addSource(new DataGeneratorSource<>(new StringGenerator(), rps, rows))
//                .returns(String.class)
//    .sinkTo(new MongoSink<>("mongodb://admin:SM67q89izW4itH7%25@192.168.220.180:27017/dev", "dev", "mycollection",
//                new StringDocumentSerializer(), properties));
//        env.execute("cdcc");
////        long rps = 50;
////        long rows = 1000L;
////        Properties properties = new Properties();
////        properties.setProperty(MongoOptions.SINK_TRANSACTION_ENABLED, "false");
////        properties.setProperty(MongoOptions.SINK_FLUSH_ON_CHECKPOINT, "false");
////        properties.setProperty(MongoOptions.SINK_FLUSH_SIZE, String.valueOf(5L));
////        properties.setProperty(MongoOptions.SINK_FLUSH_INTERVAL, String.valueOf(1_000L));
////        env.addSource(new DataGeneratorSource<>(new StringGenerator(), rps, rows))
////                .returns(String.class)
////                .sinkTo(new MongoSink<>("mongodb://admin:SM67q89izW4itH7%25@192.168.220.180:27017/dev", "dev", "sda",
////                        new StringDocumentSerializer(), properties));
////        StreamGraph streamGraph = env.getStreamGraph(MongoTransactionalSinkTest.class.getName());
////        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
////            miniCluster.start();
////            miniCluster.executeJobBlocking(streamGraph.getJobGraph());
////        }
//
//        final StreamingFileSink<String> sink = FileSink
//                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//                                .withMaxPartSize(1024 * 1024 * 1024)
//                                .build())
//                .build();
//
//    }
//
//    static class StringGenerator implements DataGenerator<String> {
//
//        private int count;
//
//        @Override
//        public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
//        }
//
//        @Override
//        public boolean hasNext() {
//            return true;
//        }
//
//        @Override
//        public String next() {
//            return "key" + count++ + "," + new Random().nextInt();
//        }
//    }
//}
