package mongoflink.source.split;

/**
 * @author chenzhuoyu
 * @date 2021/9/17 22:13
 * The mutable version of mongo split.
 **/
public class MongoSplitState extends MongoSplit {

    private long currentOffset;

    public MongoSplitState(MongoSplit mongoSplit) {
        super(mongoSplit.splitId(), mongoSplit.getQuery(), mongoSplit.getStartOffset());
        this.currentOffset = mongoSplit.getStartOffset();
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public void increaseOffset(long n) {
        currentOffset += n;
    }
}
