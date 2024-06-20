package oopproject1.admin;

import java.util.PriorityQueue;

public class KafkaReplica {

    private KafkaBroker broker;
    private KafkaPartition partition;
    private PriorityQueue<KafkaMessages.Message> messageQueue;

    public KafkaReplica(KafkaBroker broker, KafkaPartition partition) {
        this.broker = broker;
        this.partition = partition;
        this.messageQueue = new PriorityQueue<>(new KafkaMessages.MessageComparator());
    }

    public void addMessage(KafkaMessages.Message message) {
        messageQueue.offer(message);
    }

    public KafkaMessages.Message pollMessage() {
        return messageQueue.poll();
    }

    public void setOffset(long offset) {
        partition.setOffset(offset);
    }

    public long getOffset() {
        return partition.getOffset();
    }

    public KafkaMessages.Message peekMessage() {
        return messageQueue.peek();
    }

    public boolean isEmpty() {
        return messageQueue.isEmpty();
    }

    public int getMessageQueueSize() {
        return messageQueue.size();
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

    // Метод для обновления сообщений и offset в реплике
    public void updateFromPartition(KafkaPartition partition) {
        // Переносим все сообщения из раздела в реплику
        while (!partition.getMessageQueue().isEmpty()) {
            KafkaMessages.Message message = partition.pollMessage();
            addMessage(message);
        }
        // Устанавливаем offset реплики в соответствии с разделом
        setOffset(partition.getOffset());
    }
}
