### mission-service-quarkus

*Implementation notes*

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
