# python-kafka-offset-resetter

Resets a kafka offset for a consumer group, back to earliest for given topics and partitions.

You will need to provide a config.yaml similar to the example


Usage: 

```
python reset_kafka_offset.py -t=topic1,topic2,topic3 -c=config.yaml

```
