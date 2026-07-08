package com.job4me.ingest;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaOperations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class NoteControllerTest {

    @SuppressWarnings("unchecked")
    @Test
    void producesToNoteEventsKeyedBySourceId() {
        KafkaOperations<String, NoteEvent> kafkaTemplate = mock(KafkaOperations.class);
        NoteController controller = new NoteController(kafkaTemplate);

        NoteEvent event = new NoteEvent("kbd-1", "NOTE_ON", 60, 100, 123L);
        ResponseEntity<Void> response = controller.ingest(event);

        verify(kafkaTemplate).send(NoteController.NOTE_TOPIC, "kbd-1", event);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }
}
