package oopproject1.admin;


import java.util.ArrayList;
import java.util.List;

public class KafkaBroker {

	private String host;
	private static int brokerId = 0;
   private int myBrokerId;
   private int port;
   private List<KafkaTopic> topics;
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
        this.topics = new ArrayList<>();
        this.myBrokerId = ++brokerId;
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

    public List<KafkaTopic> getTopics() {
        return topics;
    }

    public void setTopics(List<KafkaTopic> topics) {
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
        if (topics.size() < maxTopics) {
            topics.add(topic);
        } else {
            throw new IllegalStateException("there is no space for topics");
        }
    }

    
    void removeTopic(String topicName) {
        for (int i = 0; i < topics.size(); i++) {
            if (topics.get(i).getName().equals(topicName)) {
               topics.remove(i);
                return;
            }
        }
        throw new IllegalArgumentException("There is no topic with such name");
    }
    
    
    void listAllTopics() {
        for (KafkaTopic topic : topics) {
            System.out.println(topic.getName());
        }
    }


        void listAllTopics(boolean includeDetails) {
            for (KafkaTopic topic : topics) {
                if (includeDetails) {
                    System.out.println("Topic Name: " + topic.getName() + ", Partitions: " + topic.getPartitions().size());
                    System.out.println("  Partitions:");
                    for (KafkaPartition partition : topic.getPartitions()) {
                        if (partition != null) {
                            System.out.println("    Partition: " + partition.toString());
                            System.out.println("      Replicas:");
                            for (KafkaReplica replica : partition.getReplicas()) {
                                if (replica != null) {
                                    System.out.println("        Replica: " + replica.getBroker().getHost() + ":" + replica.getBroker().getPort());
                                } else {
                                    System.out.println("        Replica: null");
                                }
                            }
                        } else {
                            System.out.println("    Partition: null");
                        }
                    }
                } else {
                    System.out.println(topic.getName());
                }
            }
        }
    }
