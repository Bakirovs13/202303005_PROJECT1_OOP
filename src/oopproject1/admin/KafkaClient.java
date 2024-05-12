package oopproject1.admin;

abstract class KafkaClient {

	private KafkaTopic topic;

    public KafkaClient(KafkaTopic topic) {
        if (topic == null) {
            throw new IllegalArgumentException("Topic не может быть null");
        }
        this.topic = topic;
    }



    public KafkaTopic getTopic() {
        return topic;
    }

    public void setTopic(KafkaTopic topic) {
        this.topic = topic;
    }

    
    

public  void sendMessage(String message){
      System.out.println("Sending message : " + message);
}




public  abstract String receiveMessage();

}


