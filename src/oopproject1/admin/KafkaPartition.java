package oopproject1.admin;

public class KafkaPartition {

   private long offset;
   private KafkaReplica[] replicas;
    private final static long DEFAULT_OFFSET = 0;
    private static final int DEFAULT_NUM_REPLICAS = 3;


    public KafkaPartition() {
        this.offset = DEFAULT_OFFSET;
        this.replicas = new KafkaReplica[DEFAULT_NUM_REPLICAS];
    }

    public KafkaPartition(int replicationFactor) {
        this.offset = DEFAULT_OFFSET;
        this.replicas = new KafkaReplica[replicationFactor];
    }

    public KafkaPartition(long offset, int replicationFactor) {
        this.offset = offset;
        this.replicas = new KafkaReplica[replicationFactor];
    }


    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public KafkaReplica[] getReplicas() {
        return replicas;
    }

    public void setReplicas(KafkaReplica[] replicas) {
        this.replicas = replicas;
    }

    public static long getDefaultOffset() {
        return DEFAULT_OFFSET;
    }

}
