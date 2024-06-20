
package oopproject1.frontEnd;

import oopproject1.admin.KafkaBroker;
import oopproject1.admin.KafkaCluster;
import oopproject1.admin.KafkaConsumer;
import oopproject1.admin.KafkaProducer;
import oopproject1.admin.KafkaTopic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 *
 * @author ngiatrakos
 */
public class KafkaCLI {

    public static void main(String[] args) {


        KafkaCluster cluster = init();
        runCLI(cluster);


    }


    public static KafkaCluster init() {
        int maxBrokersInCluster = 10;
        int maxTopicsPerBroker = 10;
        KafkaCluster cluster = new KafkaCluster(maxBrokersInCluster, maxTopicsPerBroker);

        try {
            for (int i = 0; i < maxBrokersInCluster; i++) {
                cluster.insertBroker(new KafkaBroker("127.0.0.1", 9090 + i, 10));
            }

            KafkaTopic[] topics = new KafkaTopic[6];

            cluster.addTopic("stock_prices", 30, 50, 20, 4, true);
            topics[0] = cluster.findTopicByName("stock_prices");

            cluster.addTopic("ship_locations", 20, 7, 12, 3, true);
            topics[1] = cluster.findTopicByName("ship_locations");

            cluster.addTopic("power_consumptions", 30, 33, 33, 4, false);
            topics[2] = cluster.findTopicByName("power_consumptions");

            cluster.addTopic("weather_conditions", 4, 3, 7, 2, true);
            topics[3] = cluster.findTopicByName("weather_conditions");

            cluster.addTopic("plane_locations", 6, 12, 12, 2, true);
            topics[4] = cluster.findTopicByName("plane_locations");

            cluster.addTopic("bookings", 4, 2, 2, 1, false);
            topics[5] = cluster.findTopicByName("bookings");

            for (KafkaTopic topic : topics) {
                if (topic == null) {
                    continue;
                }
                for (int j = 0; j < 3; j++) { // Example number of producers
                    new KafkaProducer(topic);
                }
                for (int j = 0; j < 3; j++) { // Example number of consumers
                    new KafkaConsumer(topic);
                }
            }
        } catch (Exception e) {
            System.out.println("Cluster initialization failed. Error: " + e.getMessage());
        }
        return cluster;
    }

    private static void runCLI(KafkaCluster cluster) {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Select an option:");
            System.out.println("1. Load messages from file");
            System.out.println("2. Exit");

            int choice = scanner.nextInt();
            scanner.nextLine(); // consume the newline character
            switch (choice) {
                case 1:
                    System.out.print("Enter topic name: ");
                    String topicName = scanner.nextLine();
                    KafkaTopic topic = cluster.findTopicByName(topicName);
                    if (topic != null) {
                        System.out.print("Enter file name (in ./data directory): ");
                        String fileName = scanner.nextLine();
                        loadMessagesFromFile(topic, fileName);
                    } else {
                        System.out.println("Topic not found.");
                    }
                    break;
                case 2:
                    System.out.println("Exiting...");
                    scanner.close();
                    return;
                default:
                    System.out.println("Invalid option. Please try again.");
                    break;
            }
        }
    }

    public static void loadMessagesFromFile(KafkaTopic topic, String fileName) {
        String filePath = "./data/" + fileName;
        List<String> lines = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
            return;
        }

        int numProducers = topic.getProducers().size();

        int linesPerProducer = lines.size() / numProducers;

        for (int i = 0; i < numProducers; i++) {
            KafkaProducer producer = topic.getProducers().get(i);
            int start = i * linesPerProducer;
            int end = (i == numProducers - 1) ? lines.size() : (i + 1) * linesPerProducer;
            for (int j = start; j < end; j++) {
                producer.sendMessage(lines.get(j));
            }
        }
        System.out.println("Messages loaded from " + fileName + " to topic " + topic.getName());
    }

}

