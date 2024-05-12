package oopproject1.admin;


public class KafkaCluster {

    private KafkaBroker[] brokers;
    private int default_max_brokers;
    private int brokerCount;
    private int maxBrokers;


    public KafkaCluster(int maxBrokers, int maxTopicsPerBroker) {
        this.maxBrokers = maxBrokers;
        this.default_max_brokers = maxTopicsPerBroker;
        this.brokers = new KafkaBroker[maxBrokers];
        this.brokerCount = 0;
    }

    //getters and setters

    public KafkaBroker[] getBrokers() {
        return brokers;
    }

    public void setBrokers(KafkaBroker[] brokers) {
        this.brokers = brokers;
    }

    public int getDefault_max_brokers() {
        return default_max_brokers;
    }

    public void setDefault_max_brokers(int default_max_brokers) {
        this.default_max_brokers = default_max_brokers;
    }

    public int getBrokerCount() {
        return brokerCount;
    }

    public void setBrokerCount(int brokerCount) {
        this.brokerCount = brokerCount;
    }

    public int getMaxBrokers() {
        return maxBrokers;
    }

    public void setMaxBrokers(int maxBrokers) {
        this.maxBrokers = maxBrokers;
    }

    //implementation of methods

    public void insertBroker(KafkaBroker broker) {
        if (brokerCount >= maxBrokers) {
            System.out.println("No space available to add a new broker");
            return;
        }
        if (checkBrokerExistence(broker.getHost(), broker.getPort())) {
            System.out.println("A broker with the same host and port already exists.");
            return;
        }
        brokers[brokerCount++] = broker;
        System.out.println("The broker with host " + broker.getHost() + " and port " + broker.getPort() + " was added.");
    }

    public void removeBroker(String host, int port) {
        for (int i = 0; i < brokerCount; i++) {
            if (brokers[i].getHost().equals(host) && brokers[i].getPort() == port) {
                System.arraycopy(brokers, i + 1, brokers, i, brokerCount - i - 1);
                brokers[--brokerCount] = null;
                return;
            }
        }
        throw new IllegalArgumentException("Broker is not found");
    }

   public KafkaBroker findBrokerByHostAndPort(String host, int port) {
        for (int i = 0; i < brokerCount; i++) {
            if (brokers[i] != null && brokers[i].getPort() == port && brokers[i].getHost().equals(host)) {
                return brokers[i];
            }
        }
        return null;
    }

    public void updateBrokerPort(String host, int port, int newPort) {
        KafkaBroker brokerToUpdate = findBrokerByHostAndPort(host, port);
        if (brokerToUpdate != null) {
            if (newPort == port) {
                System.out.println("Error: New port is the same as the current port of the broker.");
                return;
            }
            for (KafkaBroker broker : brokers) {
                if (broker != null && broker != brokerToUpdate && broker.getPort() == newPort) {
                    System.out.println("Error: New port " + newPort + " is already in use by another broker.");
                    return;
                }
            }
            brokerToUpdate.setPort(newPort);
            System.out.println("Broker with host " + host + " and port " + port + " was updated to port " + newPort);
        } else {
            System.out.println("Broker with host " + host + " and port " + port + " was not found.");
        }
    }

    public void addTopic(String topicName, int numPartitions, int maxProducers, int maxConsumers, int replicationFactor, boolean keyed) {
        KafkaBroker leastLoadedBroker = null;
        int minTopicCount = Integer.MAX_VALUE;
        for (KafkaBroker broker : brokers) {
            if (broker != null && broker.getTopicCount() < minTopicCount) {
                minTopicCount = broker.getTopicCount();
                leastLoadedBroker = broker;
            }
        }
        if (leastLoadedBroker != null) {
            leastLoadedBroker.addTopic(new KafkaTopic(topicName, numPartitions, leastLoadedBroker, maxProducers, maxConsumers, replicationFactor, keyed));
        } else {
            throw new IllegalStateException("No broker found");
        }
    }


    public void deleteTopic(String topicName) {
        KafkaTopic deleteTopic = findTopicByName(topicName);
        if (deleteTopic != null) {
            for (KafkaBroker broker : brokers) {
                if (broker != null) { // Check if broker is not null
                    broker.removeTopic(topicName);
                    System.out.println("Topic " + topicName + " was deleted from broker " + broker.getHost() + ":" + broker.getPort());
                } else {
                    System.err.println("Null broker found in the list."); // For debugging
                }
            }
        } else {
            System.out.println("Topic " + topicName + " does not exist.");
        }
    }

    public KafkaTopic findTopicByName(String topicName) {
        for (KafkaBroker broker : brokers) {
            KafkaTopic[] brokerTopics = broker.getTopics();
            if (brokerTopics != null) {
                for (KafkaTopic topic : brokerTopics) {
                    if (topic != null && topic.getName().equals(topicName)) {
                        return topic;
                    }
                }
            }
        }
        return null;
    }

    public void listAllBrokers() {
        if (brokers == null || brokerCount ==0 ){
            System.out.println("No brokers found in the KafkaCluster");
            return;
        }
        System.out.println("List of all brokers :");
        for (int i = 0; i < brokerCount; i++) {
            KafkaBroker currentBroker = brokers[i];
            System.out.println("Broker ID : " + currentBroker.getMyBrokerId());
            System.out.println("Broker Host : " + currentBroker.getHost());
            System.out.println("Broker Post : " + currentBroker.getPort());
            System.out.println("________________________________________");

        }
    }

    public void listAllTopicsAcrossBrokers() {
        for (KafkaBroker broker : brokers) {
            if (broker != null) {
                broker.listAllTopics();
            }
        }
    }

    public void listAllTopicsAcrossBrokers(boolean includeDetails) {
        for (KafkaBroker broker : brokers) {
            if (broker != null) {
                System.out.println("List of Topics for broker " + broker.getHost() + ":" + broker.getPort() + ":");
                broker.listAllTopics(includeDetails);
                System.out.println();
            }
        }
    }


    public boolean checkBrokerExistence(String host, int port) {
        for (int i = 0; i < brokerCount; i++) {
            if (brokers[i].getHost().equals(host) && brokers[i].getPort() == port) {
                System.out.println("The broker with host :" + host + " and port : "+port + "exists.");
                return true;
            }
        }
        return false;
    }

    public boolean checkTopicExistence(String topicName) {
        for (KafkaBroker broker : brokers) {
            if (broker != null) {
                KafkaTopic[] brokerTopics = broker.getTopics();
                if (brokerTopics != null) {
                    for (KafkaTopic topic : brokerTopics) {
                        if (topic != null && topic.getName().equals(topicName)) {
                            System.out.println("The topic with name :"+topicName + "exists.");
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}

