package oopproject1.admin;

public class KafkaTopic {

	private String name;
    private KafkaBroker owner;
   private KafkaPartition[] partitions;
   private KafkaProducer[] producers;
   private KafkaConsumer[] consumers;
   private int replicationFactor;
   private boolean keyed;
   private int producerCount;
   private int consumerCount;

    private static final int DEFAULT_REPLICATION_FACTOR = 1;
    private static final boolean DEFAULT_KEYED = false;
    private static final int DEFAULT_MAX_PRODUCERS = 10;
    private static final int DEFAULT_MAX_CONSUMERS = 10;



   public KafkaTopic(String name, int numPartitions,KafkaBroker owner, int maxProducers, int maxConsumers, int replicationFactor, boolean keyed) {
       if (replicationFactor < 1 || numPartitions <= 0 || maxProducers <= 0 || maxConsumers <= 0) {
           throw new IllegalArgumentException("Invalid arguments provided.");
       }
       checkValidPositiveInteger(numPartitions); // Validate numPartitions
       checkValidPositiveInteger(maxProducers); // Validate maxProducers
       checkValidPositiveInteger(maxConsumers); // Validate maxConsumers

       this.name = name;
       this.owner = owner;
       this.partitions = new KafkaPartition[numPartitions];
       this.producers = new KafkaProducer[maxProducers];
       this.consumers = new KafkaConsumer[maxConsumers];
       this.replicationFactor = replicationFactor;
       this.keyed = keyed;
       this.producerCount = 0;
       this.consumerCount = 0;
}
       
    // Constructor with default replication factor and keyed values
    public KafkaTopic(String name, int numPartitions, KafkaBroker owner, int maxProducers, int maxConsumers){
        this(name, numPartitions, owner, maxProducers, maxConsumers, DEFAULT_REPLICATION_FACTOR, DEFAULT_KEYED);
    }
    
    // Constructor with default max producers and consumers
  public   KafkaTopic(String name, int numPartitions, KafkaBroker owner, int replicationFactor, boolean keyed){
      this(name, numPartitions, owner, DEFAULT_MAX_PRODUCERS, DEFAULT_MAX_CONSUMERS, replicationFactor, keyed);

  }
    
    // Constructor with default replication factor, keyed, max producers, and max consumers
   public KafkaTopic(String name, int numPartitions, KafkaBroker owner){
       this(name, numPartitions, owner, DEFAULT_MAX_PRODUCERS, DEFAULT_MAX_CONSUMERS, DEFAULT_REPLICATION_FACTOR, DEFAULT_KEYED);

   }

   //getters and setters


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public KafkaBroker getOwner() {
        return owner;
    }

    public void setOwner(KafkaBroker owner) {
        this.owner = owner;
    }

    public KafkaPartition[] getPartitions() {
        return partitions;
    }

    public void setPartitions(KafkaPartition[] partitions) {
        this.partitions = partitions;
    }

    public KafkaProducer[] getProducers() {
        return producers;
    }

    public void setProducers(KafkaProducer[] producers) {
        this.producers = producers;
    }

    public KafkaConsumer[] getConsumers() {
        return consumers;
    }

    public void setConsumers(KafkaConsumer[] consumers) {
        this.consumers = consumers;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public boolean isKeyed() {
        return keyed;
    }

    public void setKeyed(boolean keyed) {
        this.keyed = keyed;
    }

    public int getProducerCount() {
        return producerCount;
    }

    public void setProducerCount(int producerCount) {
        this.producerCount = producerCount;
    }

    public int getConsumerCount() {
        return consumerCount;
    }

    public void setConsumerCount(int consumerCount) {
        this.consumerCount = consumerCount;
    }

    public static int getDefaultReplicationFactor() {
        return DEFAULT_REPLICATION_FACTOR;
    }

    public static boolean isDefaultKeyed() {
        return DEFAULT_KEYED;
    }

    public static int getDefaultMaxProducers() {
        return DEFAULT_MAX_PRODUCERS;
    }

    public static int getDefaultMaxConsumers() {
        return DEFAULT_MAX_CONSUMERS;
    }


    //methods

    void addProducer(KafkaProducer producer) {
        if (producerCount < producers.length) {
            producers[producerCount++] = producer;
        } else {
            throw new IllegalStateException("there is max number of producers");
        }
    }

    void addConsumer(KafkaConsumer consumer) {
        if (consumerCount < consumers.length) {
            consumers[consumerCount++] = consumer;
        } else {
            throw new IllegalStateException("there is max number of consumers");
        }
    }
    
    void checkValidPositiveInteger(int parameter)    {
        if (parameter < 1) {
            throw new IllegalArgumentException("The given number should be positive");
        }
    }

}
