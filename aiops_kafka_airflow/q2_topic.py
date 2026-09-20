from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(
    bootstrap_servers="localhost:9092"
)

topic = NewTopic(
    name="server_metrics",
    num_partitions=1,
    replication_factor=1
)

admin.create_topics(new_topics=[topic])

print("Topic created successfully!")

admin.close()