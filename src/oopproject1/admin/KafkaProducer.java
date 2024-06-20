package oopproject1.admin;

public class KafkaProducer {

	private KafkaTopic topic;

	public KafkaProducer(KafkaTopic topic) {
		this.topic = topic;
	}

	public void sendMessage(String value) {
		KafkaMessages.NonKeyedMessage<String> message = new KafkaMessages.NonKeyedMessage<>(value);
		topic.insertMessage(message);
	}
}
