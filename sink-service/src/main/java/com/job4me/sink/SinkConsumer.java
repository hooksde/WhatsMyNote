package com.job4me.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes both topics and writes them to whichever EventSink is active
 * (DynamoDB or Timestream, selected by Spring profile).
 *
 * Listeners take the raw JSON string and parse to the known type per topic.
 * That keeps deserializer config trivial; swap in typed deserializers or a
 * schema registry for production.
 */
@Component
public class SinkConsumer {

    private final EventSink sink;
    private final ObjectMapper mapper = new ObjectMapper();

    public SinkConsumer(EventSink sink) {
        this.sink = sink;
    }

    @KafkaListener(topics = "note-events", groupId = "note-sink")
    public void onNote(String json) throws Exception {
        sink.write(mapper.readValue(json, NoteEvent.class));
    }

    @KafkaListener(topics = "chord-events", groupId = "chord-sink")
    public void onChord(String json) throws Exception {
        sink.write(mapper.readValue(json, ChordEvent.class));
    }
}
