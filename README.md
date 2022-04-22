# White label applications

This repository contains examples of application to produce and consume to Apache Kafka.
It includes many best-practices while interacting with Apache Kafka.
Many comments have been written to explain the logic and the importance of a few parameters.

Those examples might not be perfect, but they could be used as a base source code for your application.

## Overall tips & tricks

### Use cases & Architecture

* You probably do not need a huge number of partition
* Rely on Kafka Connect connectors if you want to get or push data from / to an external system
* Monitor your application, specifically the Consumer lag
* Rely on the Schema Registry for all topics used to share data across multiple applications

### Producer

* Always think about **Data Durability**
    * `acks=all`, `enable.idempotence=true` are set by default since Kafka 3.0
    * Before Kafka 3.0, you must set those parameters explicitly if you care about data durability
* **Retries are automatically performed** by the producer since Kafka 2.1
    * The default configuration is to retry for 2 minutes (`delivery.timeout.ms`)
* **Avoid** sending messages **synchronously**, this could impact performances
* Always inject the Kafka client configuration dynamically
    * e.g. with a configuration file, a ConfigMap or environment variables
    * You might need to update unexpected parameters in production, e.g. timeout parameters

### Consumer

* You **need** some **error handling** strategy, default behavior is **log and stop everything**; there are two kind of
  exception
  that you must manage:
    * **Deserialization exception**: thrown if a message can not be deserialized properly
    * **Unexpected process exception**: if you can not process a message, e.g. by throwing a NullPointerException due to
      a bug or invalid data
    * If those exceptions are not handle, all consumer will try processing this message and crash in an infinite loop.
    * This kind of messages are called a **Poison pill**
* Consumer must be **idempotent**
    * The default processing guarantee of Apache Kafka, and most message broker, is **"at-least-once"**, thus message
      could be processed twice
    * Reaching "exactly-once" or "effectively-once" processing is feasible, but, in most case, having an idempotent
      consumer is easier to implement
* Ensure a **fast processing of each message**, long-running operations, such as the invocation of a distant Web
  Services, might generate infinite timeout and consumer rebalance
    * If you need to perform a potentially long-running operation, you should do it asynchronously
    * For asynchronous processing, be aware of https://github.com/confluentinc/parallel-consumer/
* **Be aware of the order of the messages** and do not assume that messages will be ordered by time
* **Rely on auto commit** as much as possible, commit manually only if required, e.g. due to asynchronous processing or
  exactly-once like implementation

## Links and references

* [Confluent Developer Website](https://developer.confluent.io/): contains plenty of tutorials, courses and examples to
  learn leveraging Apache Kafka. A must-know.
* [Apache Kafka documentation](https://kafka.apache.org/documentation/): page listing all parameters that could be
  tuned
* [Optimizing Kafka](https://www.confluent.io/white-paper/optimizing-your-apache-kafka-deployment/): white-paper
  explaining all
  optimal parameters that could impact durability, throughput, availability or latency
* [Confluent documentation](https://docs.confluent.io/home/overview.html): Schema Registry, ksqlDB, Ansible (cp-ansible)
  , many connectors, Kubernetes (Confluent For Kubernetes), Confluent Platform and Confluent Cloud documentation
* [Confluent support](https://support.confluent.io/): Confluent support can be used for development related questions