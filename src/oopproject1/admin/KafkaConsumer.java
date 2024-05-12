package oopproject1.admin;


public class KafkaConsumer extends KafkaClient {

	public KafkaConsumer(KafkaTopic topic) {
        super(topic);
    }


public void sendMessage(String message){
	throw new UnsupportedOperationException("Consumer cannot send the message ");

}


    

public String receiveMessage() {
	return "Message was received for the topic: " + getTopic().getName();
}
}
