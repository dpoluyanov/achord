On Java 9 performance can be even better with `--add-opens java/base/jdk.internal.misc=ALL-UNNAMED`
(because Netty allocates uninitialized arrays not filled with zeros).

Java 9 is a minimal compatible version because reactive bridge based on [JEP 266](http://openjdk.java.net/jeps/266)

# Setup
## Maven

Add dependency into your `pom.xml`
```xml
<dependency>
  <groupId>com.github.mangelion</groupId>
  <artifactId>achord</artifactId>
  <version>0.1.1</version>
</dependency>
```

## Gradle

Add following dependency into your `build.gradle` script
```groovy
compile "com.github.mangelion:achord:0.1.1"
```

# Usage

At this point only one mode is ready to using. That is outbound mode where data streams from client to server.
More options coming soon, don't hesitate to create issue if you need another one.

We suppose that only one `ClickHouseClient` per server will be used. It means that you should create only one instance for every ClickHouse cluster.

Client comes with bunch of configurable options. So start with `ClickHouseClient.bootstrap()` and change those defaults that you need.
For instance we have ClickHouse cluster defined on `awesome-ch-cluster-ea1.amazon.com` at port `8999` and want to connect as user  `customer` to database `examples` with our client like below:
```java
ClickHouseClient client = ClickHouseClient.bootstrap()
                                              .remote("awesome-ch-cluster-ea1.amazon.com", 8999)
                                              .username("customer")
                                              .database("examples");
```

This is cheap client creation that doesn't block your cpu for a long time.
After client is created we ready to use it. Let us send some numbers to server:
```java
// table created as `create table examples.numbers(date Date DEFAULT toDate(now()), number UInt32 number)`  
// Flux is a part of projectreactor library and you can use any other provider like akka-streams, rxjava2, and so on  
client.sendData("INSERT INTO examples.test(number)", Flux.range(0, 1024).map(i -> new Object[] { i }))
          .subscribe();
```
Thats all. Client prepares new connection, sends your data, and notifies subscriber with error or complete signal (no any other expected from `sendData`).

If you wish to use compression, enable it on bootstrap phase with `.compression(CompressionMethod.LZ4)` (LZ4 only one mode that is supported in this release)

We hardly work on zstd, lz4hc and different combination of this methods in our driver.

You can find more examples in `ClickHouseClientTest`.
