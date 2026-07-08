package com.job4me.ingest;

import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** The edge (MidiCaptureApp) POSTs here; we key by sourceId so per-keyboard order is preserved. */
@RestController
@RequestMapping("/api/notes")
public class NoteController {

    public static final String NOTE_TOPIC = "note-events";

    private final KafkaOperations<String, NoteEvent> kafkaTemplate;

    public NoteController(KafkaOperations<String, NoteEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public ResponseEntity<Void> ingest(@RequestBody NoteEvent event) {
        kafkaTemplate.send(NOTE_TOPIC, event.sourceId(), event);
        return ResponseEntity.accepted().build();
    }
}
