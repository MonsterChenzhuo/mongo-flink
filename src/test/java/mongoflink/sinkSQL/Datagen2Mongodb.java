package mongoflink.sinkSQL;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author chenzhuoyu
 * @date 2021/9/17 22:13
 */
public class Datagen2Mongodb {

    public static void main(String args[]) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

//        String sourceSql = "CREATE TABLE datagen (\n" +
//                " id INT,\n" +
//                " name STRING\n" +
//                ") WITH (\n" +
//                " 'connector' = 'datagen',\n" +
//                " 'rows-per-second'='1',\n" +
//                " 'fields.name.length'='10'\n" +
//                ")";
//        String sinkSql = "CREATE TABLE mongoddb (\n" +
//                "  id INT,\n" +
//                "  name STRING\n" +
//                ") WITH (\n" +
//                "  'connector' = 'mongodb',\n" +
//                "  'database'='dev',\n" +
//                "  'collection'='flink_test',\n" +
//                "  'uri'='mongodb://admin:SM67q89izW4itH7%25@192.168.220.180:27017/dev',\n" +
//                "  'maxConnectionIdleTime'='20000',\n" +
//                "  'batchSize'='1'\n" +
//                ")";
//        String insertSql = "insert into mongoddb " +
//                "select id,name " +
//                "from datagen";
//
//        tableEnvironment.executeSql(sourceSql);
//        tableEnvironment.executeSql(sinkSql);
//        tableEnvironment.executeSql(insertSql);
        DataStreamSource<WC> input = env.fromElements(new WC("Hello", 545454),
                new WC("word", 5555),
                new WC("Hello", 66666)
        );

        tableEnvironment.createTemporaryView("t_words",input,$("word"),$("ct"));


        String sinkSql = "CREATE TABLE mongodb (\n" +
                "  word STRING,\n" +
                "  ct INT\n" +
                ") WITH (\n" +
                "  'connector' = 'mongodb',\n" +
                "  'database'='dev',\n" +
                "  'collection'='flink_test',\n" +
                "  'uri'='mongodb://admin:SM67q89izW4itH7%25@192.168.220.180:27017/dev',\n" +
                "  'maxConnectionIdleTime'='20000',\n" +
                "  'batchSize'='1'\n" +
                ")";
        String insertSql = "insert into mongodb " +
                "select word,ct " +
                "from t_words";

//        tenv.executeSql(sourceSql);
        tableEnvironment.executeSql(sinkSql);
        tableEnvironment.executeSql(insertSql).print();
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class WC{
        public String word;
        public Integer ct;
    }


}
