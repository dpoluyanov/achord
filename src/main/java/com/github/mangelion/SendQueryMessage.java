package com.github.mangelion;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Camelion
 * @since 18/02/2018
 */
final class SendQueryMessage extends ClientMessage {
    private static final int INITIAL_QUERY = 0x01;
    private static final int TCP_INTERFACE = 0x01;
    private static final String OS_USERNAME = System.getProperty("user.name");
    private static final String CLIENT_HOSTNAME;
    private static final int COMPATIBLE_CLIENT_REVISION = 54327;
    private final static ByteBuf STATIC_MSG_ID_BUF;
    private final static ByteBuf STATIC_CLIENT_INFO_BUF;
    private static final int SEND_QUERY_MSG_ID = 0x01;
    private static final int QUERY_PROCESSING_STAGE_COMPLETE = 2;

    static {
        String hostname = "undetermined";
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            hostname = inetAddress.getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        CLIENT_HOSTNAME = hostname;
    }

    static {
        ByteBuf buf = null;
        try {
            buf = Unpooled.directBuffer();
            writeVarUInt(buf, SEND_QUERY_MSG_ID);
            STATIC_MSG_ID_BUF = buf.copy(0, buf.writerIndex());
        } finally {
            ReferenceCountUtil.release(buf);
        }

        try {
            buf = Unpooled.directBuffer();
            buf.writeByte(INITIAL_QUERY);
            writeStringBinary(buf, ""); // initial user
            writeStringBinary(buf, ""); // initial query id
            writeStringBinary(buf, "0.0.0.0:0"); // initial address

            buf.writeByte(TCP_INTERFACE);

            writeStringBinary(buf, OS_USERNAME);
            writeStringBinary(buf, CLIENT_HOSTNAME);
            writeStringBinary(buf, "Abaddon: Client");

            writeVarUInt(buf, 1L);
            writeVarUInt(buf, 1L);
            writeVarUInt(buf, COMPATIBLE_CLIENT_REVISION);
            STATIC_CLIENT_INFO_BUF = buf.copy(0, buf.writerIndex());
        } finally {
            ReferenceCountUtil.release(buf);
        }
    }

    private final String queryId;
    private final String query;
    private final Settings settings;
    private final Limits limits;
    private final long serverRevision;

    SendQueryMessage(String queryId, String query, Settings settings, Limits limits, long serverRevision) {
        this.queryId = queryId;
        this.query = query;
        this.settings = settings;
        this.limits = limits;
        this.serverRevision = serverRevision;
    }

    @Override
    ByteBuf createPayload(ByteBufAllocator alloc) {
        ByteBuf queryIdBuf = alloc.directBuffer();
        writeStringBinary(queryIdBuf, queryId);

        ByteBuf queryBuf = alloc.directBuffer();

        if (serverRevision >= ServerRevisions.DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO) {
            writeStringBinary(queryBuf, ""); // todo quota key
        }

        settings.write(queryBuf);
        limits.write(queryBuf);
        writeStringBinary(queryBuf, ""); // end for settings & limits

        writeVarUInt(queryBuf, QUERY_PROCESSING_STAGE_COMPLETE);
        writeVarUInt(queryBuf, settings.isCompressionEnabled()); // todo compression (0 disabled, 1 enabled)
        writeStringBinary(queryBuf, query);

        if (serverRevision > ServerRevisions.DBMS_MIN_REVISION_WITH_CLIENT_INFO) {
            return alloc.compositeDirectBuffer(4)
                    .addComponents(true, STATIC_MSG_ID_BUF.retainedSlice(), queryIdBuf, STATIC_CLIENT_INFO_BUF.retainedSlice(), queryBuf);
        } else {
            return alloc.compositeDirectBuffer(3)
                    .addComponents(true, STATIC_MSG_ID_BUF.retainedSlice(), queryIdBuf, queryBuf);
        }
    }
}
