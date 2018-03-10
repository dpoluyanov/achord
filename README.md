On Java 9 performance can be even better with `--add-opens java/base/jdk.internal.misc=ALL-UNNAMED`
(because Netty initializes uninitialized arrays not filled with zeros).

Java 9 is a minimal compatible version because reactive bridge based on [JEP 266](http://openjdk.java.net/jeps/266)

# Setup
## Maven

Add dependency into your `pom.xml`
```xml
<dependency>
  <groupId>com.github.mangelion</groupId>
  <artifactId>achord</artifactId>
  <version>0.1.0</version>
</dependency>
```

## Gradle

Add following dependency into your `build.gradle` script
```groovy
compile: "com.github.mangelion:achord:0.1.0"
```
