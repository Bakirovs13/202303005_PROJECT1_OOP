package oopproject1.admin;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class KafkaPartition {

    private long offset;
    private List<KafkaReplica> replicas;
    private final static long DEFAULT_OFFSET = 0;
    private static final int DEFAULT_NUM_REPLICAS = 3;
    private KafkaBroker broker;
    private PriorityQueue<KafkaMessages.Message> messageQueue;

    public KafkaPartition(KafkaBroker broker) {
        this.offset = DEFAULT_OFFSET;
        this.replicas = new ArrayList<>(DEFAULT_NUM_REPLICAS);
        this.broker = broker;
        for (int i = 0; i < DEFAULT_NUM_REPLICAS; i++) {
            this.replicas.add(new KafkaReplica(broker, this));
        }
        this.messageQueue = new PriorityQueue<>(new KafkaMessages.MessageComparator());
    }

    public KafkaPartition(int replicationFactor, KafkaBroker broker) {
        this.offset = DEFAULT_OFFSET;
        this.replicas = new ArrayList<>(replicationFactor);
        this.broker = broker;
        for (int i = 0; i < replicationFactor; i++) {
            this.replicas.add(new KafkaReplica(broker, this));
        }
        this.messageQueue = new PriorityQueue<>(new KafkaMessages.MessageComparator());
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public List<KafkaReplica> getReplicas() {
        return replicas;
    }

    public PriorityQueue<KafkaMessages.Message> getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(PriorityQueue<KafkaMessages.Message> messageQueue) {
        this.messageQueue = messageQueue;
    }

    public void insertMessage(KafkaMessages.Message message) {
        messageQueue.offer(message);
        replicateMessage(message);
    }

    private void replicateMessage(KafkaMessages.Message message) {
        for (KafkaReplica replica : replicas) {
            replica.addMessage(message);
        }
    }

    public KafkaMessages.Message pollMessage() {
        KafkaMessages.Message message = messageQueue.poll();
        if (message != null) {
            offset++;
            updateReplicaOffsets();
        }
        return message;
    }

    private void updateReplicaOffsets() {
        for (KafkaReplica replica : replicas) {
            replica.setOffset(offset);
        }
    }

    public void setCurrentOffset(long offset) {
        this.offset = offset;
    }

    public void setReplicas(List<KafkaReplica> replicas) {
        this.replicas = replicas;
    }

    public static long getDefaultOffset() {
        return DEFAULT_OFFSET;
    }
}
