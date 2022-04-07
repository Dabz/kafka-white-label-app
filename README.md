# White label applications

This repository contains examples of application to produce and consume to Apache Kafka.
It includes many best-practices while interacting with Apache Kafka.
Many comments are also available to explain the logic and the importance of a few parameters.

Those examples might not be perfect, but they could be used as a base source code for your application.


## Overall tips & tricks

### Use cases & Architecture

* You probably do not need a huge number of partition
* Rely on Kafka Connect connectors if you want to get or push data from / to an external system
* Monitor your application, specifically the Consumer lag
- Rely on the Schema Registry for all "public" or "shared" topics

### Producer

* Always think about Data Durability (`acks=all`, `enable.idempotence=true` by default since Kafka 3.0)
- Retries are automatically performed by the producer (`delivery.timeout=2min` by default)
- Avoid sending messages synchronously

### Consumer

* You probably need some error handling, most default behavior is "log and stop everything"
* Consumer must be idempotent
* Be aware of the order of the messages and do not assume that messages will be ordered by time
- Rely on auto commit as much as possible, commit manually only if required (e.g. due to asynchronous processing or exactly-once like implementation)
- For asynchronous processing, be aware of https://github.com/confluentinc/parallel-consumer/ 
