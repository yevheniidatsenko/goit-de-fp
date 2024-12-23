from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Creating a Kafka client
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Defining new topics
my_name = "zhenya_datsenko"
athlete_event_results_name = f'{my_name}_athlete_event_results'
athlete_avg_stats_name = f'{my_name}_athlete_avg_stats'
num_partitions = 2
replication_factor = 1

# Creating NewTopic objects
athlete_event_results = NewTopic(name=athlete_event_results_name, num_partitions=num_partitions, replication_factor=replication_factor)
athlete_avg_stats = NewTopic(name=athlete_avg_stats_name, num_partitions=num_partitions, replication_factor=replication_factor)

# Deleting old topics
try:
    admin_client.delete_topics(topics=[athlete_event_results_name, athlete_avg_stats_name])
    print("Topics successfully deleted.")
except Exception as e:
    print(f"Error while deleting topics: {e}")

# Creating new topics
try:
    admin_client.create_topics(new_topics=[athlete_event_results, athlete_avg_stats], validate_only=False)
    print(f"Topic '{athlete_event_results_name}' created successfully.")
    print(f"Topic '{athlete_avg_stats_name}' created successfully.")
except Exception as e:
    print(f"Error occurred while creating topics: {e}")

# Closing the client connection
admin_client.close()