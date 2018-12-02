package com.dubovskyi.streaming.kafka.client;

import com.dubovskyi.streaming.kafka.client.domain.event.Event;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AvroHelper {
    public static final DatumReader<Event> DATUM_READER = new SpecificDatumReader<>(Event.class);
    public static final DatumWriter<Event> DATUM_WRITER = new SpecificDatumWriter<>(Event.class);

    private static class Splitter extends Spliterators.AbstractSpliterator<Event> implements Closeable {
        final DataFileReader<Event> upReader;
        public Splitter(final byte[] buf) throws IOException {
            super(Long.MAX_VALUE, 0);
            upReader = new DataFileReader<>(new SeekableByteArrayInput(buf), DATUM_READER);
        }

        @Override
        public void close() throws IOException {
            upReader.close();
        }

        @Override
        public boolean tryAdvance(Consumer<? super Event> action) {
            if (upReader.hasNext()) {
                action.accept(upReader.next());
                return true;
            }
            return false;
        }
    }

    public static Event deserialize(final byte[] buf) throws IOException {
        final DataFileReader<Event> upReader = new DataFileReader<>(new SeekableByteArrayInput(buf), DATUM_READER);
        return upReader.next();
    }

    public static byte[] serialize(Event uts) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final DataFileWriter<Event> writer = new DataFileWriter<>(DATUM_WRITER)) {
            writer.create(new Event().getSchema(), out);
            writer.append(uts);

        }
        return out.toByteArray();
    }
}
