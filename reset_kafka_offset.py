import sys
from typing import List
import confluent_kafka
import yaml
from confluent_kafka import TopicPartition, OFFSET_BEGINNING, OFFSET_END


def main(argv):
    topics = ''
    config = ''
    seek = ''

    try:
        index = 0
        for arg in argv:
            index += 1
            if arg == "-t":
                topics = argv[index].split(',')

            if arg == "-c":
                config = argv[index]

            if arg == "-s":
                seek = argv[index]

    except Exception:
        print('USAGE: python reset_kafka_offset.py -c config_file -t topics -s EARLIEST')
        sys.exit(2)

    if topics == '' or config == '' or seek == '':
        print('Some parameters were not supplied')
        print('USAGE: python reset_kafka_offset.py -c config_file -t topics -s EARLIEST')
        sys.exit(3)

    print('Topics are ', topics)
    print('Config file is located ', config)
    print(f'This script will seek a kafka consumer to a specific offset of {seek}')
    result = input('Press `y` to continue')
    if result != 'y':
        print('Cancelled')
        sys.exit(1)

    seek_to(topics, config, seek)


def seek_to(topics: List[str], config_path: str, seek: str):

    if seek == 'LATEST':
        seek = OFFSET_END
    if seek == 'EARLIEST':
        seek = OFFSET_BEGINNING
    print(f'Seek has been transformed to `{seek}`')

    # load the config file
    with open(config_path, 'r') as stream:
        config_properties = yaml.safe_load(stream)

    consumer = confluent_kafka.Consumer(config_properties)
    consumer.subscribe(topics)
    for topic in topics:
        kafka_topic = consumer.list_topics().topics[topic]
        for partition in kafka_topic.partitions:
            kafka_partition = TopicPartition(topic=topic, partition=partition, offset=seek)
            print(f'Current Position: {consumer.position([kafka_partition])}')
            consumer.assign([kafka_partition])
            consumer.seek(kafka_partition)
            consumer.commit()
            print(f'New Position: {consumer.position([kafka_partition])}')


if __name__ == "__main__":
    main(sys.argv[1:])
