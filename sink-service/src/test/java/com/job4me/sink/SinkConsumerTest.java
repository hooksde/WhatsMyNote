package com.job4me.sink;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SinkConsumerTest {

    @Test
    void parsesAndWritesNoteEvents() throws Exception {
        EventSink sink = mock(EventSink.class);
        SinkConsumer consumer = new SinkConsumer(sink);

        consumer.onNote("""
                {"sourceId":"kbd-1","type":"NOTE_ON","note":60,"velocity":100,"timestamp":123}""");

        verify(sink).write(new NoteEvent("kbd-1", "NOTE_ON", 60, 100, 123));
    }

    @Test
    void parsesAndWritesChordEvents() throws Exception {
        EventSink sink = mock(EventSink.class);
        SinkConsumer consumer = new SinkConsumer(sink);

        consumer.onChord("""
                {"sourceId":"kbd-1","notes":[60,64,67],"startTs":100,"endTs":200}""");

        verify(sink).write(new ChordEvent("kbd-1", List.of(60, 64, 67), 100, 200));
    }
}
