On Java 9 performance can be even better with `--add-opens java/base/jdk.internal.misc=ALL-UNNAMED`
(because Netty initializes uninitialized arrays not filled with zero).