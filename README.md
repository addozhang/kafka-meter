# Kafka meter for JMeter

This is a kafka sampler for testing different producer config.

**Note it runs in single thread.**

## How to use

### Build shade jar

    Run `maven clean package`.
    
    The output jar carries all dependencies in.

### Add ext jar

    Copy `kafka-meter-1.0-SNAPSHOT.jar` to `JMETER_HOME/lib/ext`
    
### Create Plan

    Add a Java Request with class `KafkaSampler`    

### Setting

- kafka_brokers

    Kafka brokers, such as "192.168.2.31:9092"

- kafka_topic

    The topic which producer will send to, such as "Test"

- kafka_key_size

    The record key length, such as 100

- kafka_message_size

    The record payload length, such as 1024

- kafka_message_serializer

    The message serializer, such as "org.apache.kafka.common.serialization.ByteArraySerializer"
    
- kafka_key_serializer

    The key serializer, such as "org.apache.kafka.common.serialization.ByteArraySerializer"

- kafka_ack_mode
    
    The "ack setting, should be less than broker size. such as "-1", "1", "all"

- kafka_linger_ms

    The "linger.ms" setting, default is 0 if left empty. Accepts int value

- kafka_buffer_memory_int
    
    The "buffer.memory" setting, default is 33554432 bytes if left empty. Accepts int value.

- kafka_batch_size_int

    The "batch.size" setting, default is 16348 if left empty. Accepts int value.

- kafka_max_request_size_int

    The "max.request.size" setting, default is 1048576 bytes if left empty. Accepts int value.

- kafka_producer_ack

    How client confirms the record sent acknowledge. If set as true, it will use Future.get() to blocking sending thread. 
    
    Default is false if left empty. Accepts true or false.
    

- kafka_producer_callback

    If use callback to confirm the record sent acknowledge. This is conflicted to `kafka_producer_ack` setting. At same time, one of them can be set as true.
    
    Default is false if left empty. Accepts true or false.

