module achord {
    requires io.netty.codec;
    requires io.netty.common;
    requires io.netty.buffer;
    requires io.netty.transport;
    requires lz4.java;
    requires jctools.core;

    exports com.github.mangelion.achord;
}