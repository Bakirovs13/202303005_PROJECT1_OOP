package oopproject1.admin;

import java.util.Comparator;

public class KafkaMessages {

    public static class Message {
        private long ingestionTime; // время входа

        public Message() {
            this.ingestionTime = System.currentTimeMillis();
        }

        public long getIngestionTime() {
            return ingestionTime;
        }
    }

    public static class KeyedMessage<K, V> extends Message {
        private K key;
        private V value;

        public KeyedMessage(K key, V value) {
            super();
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }

    public static class NonKeyedMessage<V> extends Message {
        private V value;

        public NonKeyedMessage(V value) {
            super();
            this.value = value;
        }

        public V getValue() {
            return value;
        }
    }
    public static class MessageComparator implements Comparator<Message> {
        @Override
        public int compare(Message m1, Message m2) {
            return Long.compare(m1.getIngestionTime(), m2.getIngestionTime());
        }
    }

}
