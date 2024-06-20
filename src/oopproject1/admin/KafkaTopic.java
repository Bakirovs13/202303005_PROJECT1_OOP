package oopproject1.admin;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.hash;

public class KafkaTopic {

    private String name;
    private KafkaBroker owner;
    private List<KafkaPartition> partitions;
    private List<KafkaProducer> producers;
    private List<KafkaConsumer> consumers;
    private int replicationFactor;
    private boolean keyed;
    private static final int DEFAULT_REPLICATION_FACTOR = 1;
    private static final boolean DEFAULT_KEYED = false;
    private static final int DEFAULT_MAX_PRODUCERS = 10;
    private static final int DEFAULT_MAX_CONSUMERS = 10;

    public KafkaTopic(String name, int numPartitions, KafkaBroker owner, int maxProducers, int maxConsumers, int replicationFactor, boolean keyed) {
        if (replicationFactor < 1 || numPartitions <= 0 || maxProducers <= 0 || maxConsumers <= 0) {
            throw new IllegalArgumentException("Invalid arguments provided.");
        }
        checkValidPositiveInteger(numPartitions); // Validate numPartitions
        checkValidPositiveInteger(maxProducers); // Validate maxProducers
        checkValidPositiveInteger(maxConsumers); // Validate maxConsumers

        this.name = name;
        this.owner = owner;
        this.partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            this.partitions.add(new KafkaPartition(owner));
        }
        this.producers = new ArrayList<>(maxProducers);
        this.consumers = new ArrayList<>(maxConsumers);
        this.replicationFactor = replicationFactor;
        this.keyed = keyed;
    }

    // Constructors with default values
    public KafkaTopic(String name, int numPartitions, KafkaBroker owner, int maxProducers, int maxConsumers) {
        this(name, numPartitions, owner, maxProducers, maxConsumers, DEFAULT_REPLICATION_FACTOR, DEFAULT_KEYED);
    }

    public KafkaTopic(String name, int numPartitions, KafkaBroker owner, int replicationFactor, boolean keyed) {
        this(name, numPartitions, owner, DEFAULT_MAX_PRODUCERS, DEFAULT_MAX_CONSUMERS, replicationFactor, keyed);
    }

    public KafkaTopic(String name, int numPartitions, KafkaBroker owner) {
        this(name, numPartitions, owner, DEFAULT_MAX_PRODUCERS, DEFAULT_MAX_CONSUMERS, DEFAULT_REPLICATION_FACTOR, DEFAULT_KEYED);
    }

    // Getters and Setters
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

    public List<KafkaPartition> getPartitions() {
        return new ArrayList<>(partitions); // Return a copy of the list to avoid direct manipulation
    }

    public List<KafkaProducer> getProducers() {
        return new ArrayList<>(producers); // Return a copy of the list to avoid direct manipulation
    }

    public List<KafkaConsumer> getConsumers() {
        return new ArrayList<>(consumers); // Return a copy of the list to avoid direct manipulation
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
        return producers.size();
    }

    public int getConsumerCount() {
        return consumers.size();
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

    // Methods
    void addProducer(KafkaProducer producer) {
        if (producers.size() < DEFAULT_MAX_PRODUCERS) {
            producers.add(producer);
        } else {
            throw new IllegalStateException("Maximum number of producers reached.");
        }
    }

    void addConsumer(KafkaConsumer consumer) {
        if (consumers.size() < DEFAULT_MAX_CONSUMERS) {
            consumers.add(consumer);
        } else {
            throw new IllegalStateException("Maximum number of consumers reached.");
        }
    }

    private void checkValidPositiveInteger(int parameter) {
        if (parameter < 1) {
            throw new IllegalArgumentException("The given number should be positive.");
        }
    }

    // Method to insert message into appropriate partition based on key (if keyed) or round-robin (if non-keyed)
    public void insertMessage(KafkaMessages.Message message) {
        if (message instanceof KafkaMessages.KeyedMessage) {
            KafkaMessages.KeyedMessage<?, ?> keyedMessage = (KafkaMessages.KeyedMessage<?, ?>) message;
            int partitionIndex = hash(keyedMessage.getKey(), partitions.size());
            KafkaPartition partition = partitions.get(partitionIndex);
            partition.insertMessage(message);
        } else if (message instanceof KafkaMessages.NonKeyedMessage) {
            int partitionIndex = getRoundRobinPartitionIndex();
            KafkaPartition partition = partitions.get(partitionIndex);
            partition.insertMessage(message);
        } else {
            throw new IllegalArgumentException("Unsupported message type.");
        }
    }


    // Method to get the index of partition for round-robin distribution
    private int getRoundRobinPartitionIndex() {
        int partitionIndex = getProducerCount() % partitions.size();
        return partitionIndex;
    }

    // Static method to distribute partitions among consumers
    public static List<List<Integer>> distributePartitions(int numPartitions, int numConsumers) {
        List<List<Integer>> partitionRanges = new ArrayList<>();
        if (numPartitions >= numConsumers) {
            int partitionsPerConsumer = numPartitions / numConsumers;
            int remainder = numPartitions % numConsumers;
            int startPartition = 0;
            for (int i = 0; i < numConsumers; i++) {
                int endPartition = startPartition + (remainder-- > 0 ? partitionsPerConsumer : partitionsPerConsumer - 1);
                List<Integer> range = new ArrayList<>();
                for (int partition = startPartition; partition <= endPartition; partition++) {
                    range.add(partition);
                }
                partitionRanges.add(range);
                startPartition = endPartition + 1;
            }
        }
        return partitionRanges;
    }

}
