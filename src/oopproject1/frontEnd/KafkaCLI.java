
package oopproject1.frontEnd;

import oopproject1.admin.KafkaBroker;
import oopproject1.admin.KafkaCluster;
import oopproject1.admin.KafkaConsumer;
import oopproject1.admin.KafkaProducer;
import oopproject1.admin.KafkaTopic;

/**
 *
 * @author ngiatrakos
 */
public class KafkaCLI {
        
    public static void main (String [] args){
        // Create a KafkaCluster with a maximum of 5 brokers and 10 topics per broker
        KafkaCluster kafkaCluster = new KafkaCluster(5, 10);
        
        try {
            // Add brokers to the cluster
            kafkaCluster.insertBroker(new KafkaBroker("127.0.0.1", 9092, 5));
            kafkaCluster.insertBroker(new KafkaBroker("127.0.1.0", 9092, 10));
            kafkaCluster.insertBroker(new KafkaBroker("127.0.0.1", 9094, 5));
        } catch (Exception e) {
            System.out.println("Error occurred while adding brokers: " + e.getMessage());
        }
        // Attempt to insert a broker with the same host and port (duplicate)
        System.out.println("____________________________________________");
        try {
                kafkaCluster.insertBroker(new KafkaBroker("127.0.0.1", 9092, 10));
            } catch (Exception e) {
                System.err.println("Error occurred while adding broker: " + e.getMessage());
            }
        // Change the port of the second broker to conflict with another broker
        System.out.println("____________________________________________");
        try {
            kafkaCluster.updateBrokerPort("127.0.0.1", 9094, 9092);
        }catch (Exception e) {
                System.err.println("Error occurred while changing the port of a broker: " + e.getMessage());
        }  
        
        // Change the port of the second broker successfully this time
        System.out.println("____________________________________________");
        try {
            kafkaCluster.updateBrokerPort("127.0.0.1", 9094, 9093);
        }catch (Exception e) {
                System.err.println("Error occurred while changing the port of a broker: " + e.getMessage());
        } 
        
        // List all brokers in the cluster
        System.out.println("____________________________________________");
        kafkaCluster.listAllBrokers();
        
        try {
            System.out.println("____________________________________________");
            // Create topics for financial domain
            kafkaCluster.addTopic("stock_prices", 5, 10, 10, 3, true); // Example financial topic
            kafkaCluster.addTopic("trading_data", 10, 10, 10, 3, false); // Another financial topic

            System.out.println("____________________________________________");
            // Create topics for maritime domain
            kafkaCluster.addTopic("ship_locations", 3, 5, 5, 3, false); // Example maritime topic
            kafkaCluster.addTopic("weather_reports", 5, 5, 5, 3, false); // Another maritime topic

            System.out.println("____________________________________________");
            // Attempt to add a topic that already exists
            kafkaCluster.addTopic("stock_prices", 2, 2, 2, 1, false); // Attempt to add existing topic
        } catch (Exception e) {
            System.err.println("Error occurred while creating topic(s) " + e.getMessage());
        }
        
        try {
            // Attempt to add a topic with replication factor greater than the number of brokers
            kafkaCluster.addTopic("random_name", 2, 2, 2, 5, false);
        } catch (Exception ex) {
            System.err.println("Error occurred while creating topic(s) " + ex.getMessage());
        }
        
        // List all topics across brokers
        kafkaCluster.listAllTopicsAcrossBrokers(true); // Passing true to include details
        
        //check if broker exists
        kafkaCluster.checkBrokerExistence("197.28.14.197", 9092);
        //check if topic exists
        kafkaCluster.checkTopicExistence("satellite_data");

        try {
            kafkaCluster.removeBroker("127.0.0.1", 9092); //a broker that exists
            kafkaCluster.removeBroker("197.28.14.197", 9092); //a broker that does not exist
        } catch (Exception exe) {
            System.err.println("Error occurred deleting broker " + exe.getMessage());
        }
        // List all brokers in the cluster
        kafkaCluster.listAllBrokers();
        // List all topics across brokers
        kafkaCluster.listAllTopicsAcrossBrokers(); 
        
        try {
            //delete a topic that does exist
            kafkaCluster.deleteTopic("ship_locations");
            //delete a topic that does not exist
            kafkaCluster.deleteTopic("satellite_data");
        } catch (Exception exx) {
            System.err.println("Error occurred while deleting topic(s) " + exx.getMessage());
        }
        
        // List all topics across brokers wtih details
        kafkaCluster.listAllTopicsAcrossBrokers(true); 
        
        // Instantiate producers and consumers for the desired topics
        // Add producers and consumers for topics
        try {
            // Attempt to find existing topics
            KafkaTopic financialTopic = kafkaCluster.findTopicByName("stock_prices");
            KafkaTopic maritimeTopic = kafkaCluster.findTopicByName("ship_locations");
            
            // Create producers and consumers for financial topic
            if (financialTopic != null) {
                KafkaProducer financialProducer = new KafkaProducer(financialTopic);
                KafkaConsumer financialConsumer = new KafkaConsumer(financialTopic);
                
                financialProducer.sendMessage("Financial data message 1");
                System.out.println(financialConsumer.receiveMessage());
                
                financialProducer.sendMessage("Financial data message 2");
                System.out.println(financialConsumer.receiveMessage());
            } else {
                System.out.println("Financial topic not found. Cannot create producer or consumer.");
            }
            
            // Create producers and consumers for maritime topic
            if (maritimeTopic != null) {
                KafkaProducer maritimeProducer = new KafkaProducer(maritimeTopic);
                KafkaConsumer maritimeConsumer = new KafkaConsumer(maritimeTopic);
                
                maritimeProducer.sendMessage("Maritime data message 1");
                System.out.println(maritimeConsumer.receiveMessage());
                
                maritimeProducer.sendMessage("Maritime data message 2");
                System.out.println(maritimeConsumer.receiveMessage());
            } else {
                System.out.println("Maritime topic not found. Cannot create producer or consumer.");
            }
        } catch (Exception e) {
            System.err.println("Error occurred while creating producer or consumer: " + e.getMessage());
        }

        
    }
    
}
