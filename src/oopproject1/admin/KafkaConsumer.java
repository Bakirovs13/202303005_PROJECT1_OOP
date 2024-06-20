package oopproject1.admin;

import java.util.ArrayList;
import java.util.List;

public class KafkaConsumer {

	private String groupId;
	private KafkaTopic topic;
	private List<KafkaPartition> assignedPartitions;

	public KafkaConsumer(KafkaTopic topic) {
		this.groupId = "defaultGroup";
		this.topic = topic;
		this.assignedPartitions = new ArrayList<>(topic.getPartitions());
	}

	public String receiveMessage() {
		for (KafkaPartition partition : assignedPartitions) {
			KafkaMessages.Message message = partition.pollMessage(); // Используем pollMessage() для извлечения сообщения
			if (message != null) {
				updateReplicas(partition);
				return processMessage(message);
			}
		}
		return null; // Если сообщений нет
	}

	private String processMessage(KafkaMessages.Message message) {
		return "Processing message: " + message.getIngestionTime();
	}

	private void updateReplicas(KafkaPartition partition) {
		for (KafkaReplica replica : partition.getReplicas()) {
			replica.setOffset(partition.getOffset());
		}
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public List<KafkaPartition> getAssignedPartitions() {
		return assignedPartitions;
	}

	public void setAssignedPartitions(List<KafkaPartition> assignedPartitions) {
		this.assignedPartitions = assignedPartitions;
	}
}
