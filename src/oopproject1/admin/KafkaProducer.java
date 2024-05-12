package oopproject1.admin;


public class KafkaProducer extends KafkaClient {

	public KafkaProducer(KafkaTopic topic) {
        super(topic);
    }

    

public void sendMessage(String message){
	KafkaTopic topic = getTopic();
	if (topic != null) {
		System.out.println("Message sent to topic '" + topic.getName() + "': " + message);
	} else {
		System.out.println("Error: KafkaProducer is not associated with any topic.");
	}
}



public String receiveMessage(){
	throw new UnsupportedOperationException("Producer cannot receive messages");

}
}


