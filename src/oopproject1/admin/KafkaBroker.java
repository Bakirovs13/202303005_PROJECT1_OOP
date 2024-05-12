package oopproject1.admin;


public class KafkaBroker {

	private String host;
	private static int brokerId = 0;
   private int myBrokerId;
   private int port;
   private KafkaTopic[] topics;
   private int topicCount;
  private  int maxTopics;

    
    public KafkaBroker(String host, int port, int maxTopics) {
        if (!isValidHost(host)) {
            throw new IllegalArgumentException("Invalid host address");
        }
        if (!isValidPort(port)) {
            throw new IllegalArgumentException("Invalid port number");
        }
        this.host = host;
        this.port = port;
        this.maxTopics = maxTopics;
        this.topics = new KafkaTopic[maxTopics];
        this.myBrokerId = ++brokerId;
        this.topicCount = 0;
    }


//setter and getter

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public static int getBrokerId() {
        return brokerId;
    }

    public static void setBrokerId(int brokerId) {
        KafkaBroker.brokerId = brokerId;
    }

    public int getMyBrokerId() {
        return myBrokerId;
    }

    public void setMyBrokerId(int myBrokerId) {
        this.myBrokerId = myBrokerId;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public KafkaTopic[] getTopics() {
        return topics;
    }

    public void setTopics(KafkaTopic[] topics) {
        this.topics = topics;
    }

    public int getTopicCount() {
        return topicCount;
    }

    public void setTopicCount(int topicCount) {
        this.topicCount = topicCount;
    }

    public int getMaxTopics() {
        return maxTopics;
    }

    public void setMaxTopics(int maxTopics) {
        this.maxTopics = maxTopics;
    }



    boolean isValidPort(int port) {
        return port >= 0 && port <= 65535;
    }


    boolean isValidHost(String host) {
        String[] parts = host.split("\\.");
        if (parts.length != 4) {
            return false;
        }
        for (String part : parts) {
            try {
                int num = Integer.parseInt(part);
                if (num < 0 || num > 255) {
                    return false;
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    
    void addTopic(KafkaTopic topic) {
        if (topicCount < maxTopics) {
            topics[topicCount++] = topic;
        } else {
            throw new IllegalStateException("there is no space for topics");
        }
    }

    
    void removeTopic(String topicName) {
        for (int i = 0; i < topicCount; i++) {
            if (topics[i].getName().equals(topicName)) {
                System.arraycopy(topics, i + 1, topics, i, topicCount - i - 1);
                topics[--topicCount] = null;
                return;
            }
        }
        throw new IllegalArgumentException("There is no topic with such name");
    }
    
    
    void listAllTopics() {
        for (int i = 0; i < topicCount; i++) {
            System.out.println(topics[i].getName());
        }
    }


    void listAllTopics(boolean includeDetails) {
        for (int i = 0; i < topicCount; i++) {
            if (includeDetails) {
                System.out.println("Topic Name: " + topics[i].getName() + ", Partitions: " + topics[i].getPartitions().length);
                System.out.println("  Partitions:");
                for (int j = 0; j < topics[i].getPartitions().length; j++) {
                    KafkaPartition partition = topics[i].getPartitions()[j];
                    if (partition != null) {
                        System.out.println("    Partition " + (j + 1) + ":");
                        System.out.println("      Replicas:");
                        for (int k = 0; k < partition.getReplicas().length; k++) {
                            KafkaReplica replica = partition.getReplicas()[k];
                            if (replica != null) {
                                System.out.println("        Replica " + (k + 1) + ": " + replica.getBroker().getHost() + ":" + replica.getBroker().getPort());
                            } else {
                                System.out.println("        Replica " + (k + 1) + ": null");
                            }
                        }
                    } else {
                        System.out.println("    Partition " + (j + 1) + ": null");
                    }
                }
            } else {
                System.out.println(topics[i].getName());
            }
        }
    }
}
