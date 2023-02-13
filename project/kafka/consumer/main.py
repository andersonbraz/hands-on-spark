from confluent_kafka import Consumer


def run_consumer():
    consumer = Consumer(
        {
            'bootstrap.servers':'kafka.uatdata.local:31090',
            'group.id':'python-consumer'
        }
    )
    result = consumer.list_topics().topics
    print(result)

if __name__ == "__main__":
    run_consumer()
