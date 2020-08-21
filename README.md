### mission-service-quarkus

Service responsible for the management of *missions*.
Implemented with Quarkus.

**Implementation notes**

_Dependency of unit tests to a running Infinispan instance_

When using the Quarkus Infinispan client extension, unit tests with QuarkusTest expect a running Infinispan server, even if the CDI Bean managing the cache is mocked out or replaced by a CDI alternative.

The root cause is within the initialization code of the extension, in the `io.quarkus.infinispan.client.runtime.InfinispanClientProducer` class: when using the default `ProtoStreamMarshaller` marshaller, the HotRod client tries to put the protostream proto files in the cache.
 
Also, typically the remote cache is configured in a startup method of a CDI Bean, which will also be executed in the QuarkusTest.

The solution applied here consists of:
* configure the hotrod client with a mock marshaller in the test profile, in `src/test/META-INF/hotrod-client.properties`.
  ```
  infinispan.client.hotrod.marshaller=com.redhat.emergency.response.repository.MockMarshaller
  ```
* not initializing the cache in e.g. tests, by using a boolean feature flag in the startup method:
  ```
      void onStart(@Observes StartupEvent e) {
          // do not initialize the cache at startup when remote cache is not available, e.g. in QuarkusTests
          if (!lazy) {
              log.info("Creating remote cache");
              missionCache = initCache();
          }
      }
  ```
  
 _Error handling with Reactive Messaging and Kafka_
 
 The Smallrye Reactive Messaging for Kafka has a number of built-in failure strategies for when a message cannot be acked.
 These include `ignore`, `fail`, `dead-letter-queue`. The default is `fail`, which means that the Kafka consumer will stop consuming messages. The same happens when uncaught exceptions are bubbled up to the consumer.
 
 The mission service has a custom implementation for error handling when consuming _CreateMissionCommand_ messages.  
 In case of an error (e.g. the MapBox API is down, or there is an issue with Data Grid), the consumer for the partition of the faulty message is paused, and the message is not acked.  
 After a configurable delay, the consumer is resumed again, starting to consume from the message that caused the error. If the error situation is still present, the consumer is again paused. 
 In order to avoid a retry storm, the pause delay increases until a maximum length.
 
