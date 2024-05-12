package oopproject1.admin;

public class KafkaReplica {

	private KafkaBroker broker;
   private KafkaPartition partition;

   public KafkaReplica(KafkaBroker broker, KafkaPartition partition) {
        this.broker = broker;
        this.partition = partition;
    }



    public KafkaBroker getBroker() {
        return broker;
    }

    public void setBroker(KafkaBroker broker) {
        this.broker = broker;
    }

    public KafkaPartition getPartition() {
        return partition;
    }

    public void setPartition(KafkaPartition partition) {
        this.partition = partition;
    }
}
