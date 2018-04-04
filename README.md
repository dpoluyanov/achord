[![Build Status](https://travis-ci.org/Mangelion/achord.svg?branch=master)](https://travis-ci.org/Mangelion/achord)

Java 10 is a minimal compatible version because reactive bridge based on [JEP 266](http://openjdk.java.net/jeps/266)
and Java 9 reached EOL, so we switched to actual 10 version starting from 0.2.0 driver version.

# Setup
## Maven

Add dependency into your `pom.xml`
```xml
<dependency>
  <groupId>com.github.mangelion</groupId>
  <artifactId>achord</artifactId>
  <version>0.2.0</version>
</dependency>
```

## Gradle

Add following dependency into your `build.gradle` script
```groovy
compile "com.github.mangelion:achord:0.2.0"
```

# Usage

At this point only one mode is ready to using. That is outbound mode where data streams from client to server.
More options coming soon, don't hesitate to create issue if you need another one.

We suppose that only one `ClickHouseClient` per server will be used. It means that you should create only one instance for every ClickHouse cluster.

Client comes with bunch of configurable options. So start with `ClickHouseClient.bootstrap()` and change those defaults that you need.
For instance we have ClickHouse cluster defined on `awesome-ch-cluster-ea1.amazon.com` at port `8999` and want to connect as user  `customer` to database `examples` with our client like below:
```java
var client = ClickHouseClient.bootstrap()
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

# OS Based Network stack.

As Netty provides KQueue/Epool based mechanism like drop-in-replacement of Java.NIO interface, `Achord` tries to use it whenever it possible.
First of all, it looks into Epool/Kqueue classes that might be presented in classpath. (Please, consult with [Netty documentation](https://netty.io/wiki/native-transports.html#wiki-h3-2) if you wish to add it manually.)

When there are no epool/kqueue libraries in classpath `Achord` recedes onto Java.NIO if  `strictNativeNetwork` option not used, or set to `false`.
