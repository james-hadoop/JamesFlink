package com.james.flink.kafka;

import com.james.flink.kafka.event.Event;
import com.james.flink.kafka.event.EventType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A serializer and deserializer for the {@link Event} type.
 */
public class EventDeSerializer implements DeserializationSchema<Event>, SerializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    @Override
    public byte[] serialize(Event evt) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(0, evt.sourceAddress());
        byteBuffer.putInt(4, evt.type().ordinal());
        return byteBuffer.array();
    }

    @Override
    public Event deserialize(byte[] message) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(message).order(ByteOrder.LITTLE_ENDIAN);
        int address = buffer.getInt(0);
        int typeOrdinal = buffer.getInt(4);
        return new Event(EventType.values()[typeOrdinal], address);
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
