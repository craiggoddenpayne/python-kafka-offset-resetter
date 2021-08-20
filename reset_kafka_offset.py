import getopt
import sys
from typing import List

import confluent_kafka
import yaml
from confluent_kafka import TopicPartition, OFFSET_BEGINNING


def main(argv):
    topics = ''
    config = ''

    try:
        index = 0
        for arg in argv:
            index += 1
            if arg == "-t":
                topics = argv[index].split(',')

            if arg == "-c":
                config = argv[index]

    except Exception:
        print('reset_kafka_offset.py -t=topics -c=config_file')
        sys.exit(2)

    print('Topics are ', topics)
    print('Config file is located ', config)
    print('This script will seek a kafka consumer to a specific offset of EARLIEST')
    result = input('Press `y` to continue')
    if result != 'y':
        sys.exit(1)

    reset_stream_to_0(topics, config)


def reset_stream_to_0(topics: List[str], config_path: str):
    # load the config file
    config_properties = {}
    with open(config_path, 'r') as stream:
        config_properties = yaml.safe_load(stream)

    consumer = confluent_kafka.Consumer(config_properties)
    consumer.subscribe(topics)
    for topic in topics:
        kafka_topic = consumer.list_topics().topics[topic]
        for partition in kafka_topic.partitions:
            kafka_partition = TopicPartition(topic=topic, partition=partition, offset=OFFSET_BEGINNING)
            consumer.assign([kafka_partition])
            consumer.seek(kafka_partition)
            consumer.commit()
            print(f'Reset offset to earliest for {topic}:{partition}')


if __name__ == "__main__":
    main(sys.argv[1:])
