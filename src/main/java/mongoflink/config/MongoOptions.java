package mongoflink.config;

import java.io.Serializable;

/**
 * @author chenzhuoyu
 * @date 2021/9/17 22:13
 * Config options for {@link mongoflink.sink.MongoSink}.
 **/
public class MongoOptions implements Serializable {

    public static final String SINK_TRANSACTION_ENABLED = "sinkSQL.transaction.enable";

    public static final String SINK_FLUSH_ON_CHECKPOINT = "sinkSQL.flush.on-checkpoint";

    public static final String SINK_FLUSH_SIZE = "sinkSQL.flush.size";

    public static final String SINK_FLUSH_INTERVAL = "sinkSQL.flush.interval";

}
