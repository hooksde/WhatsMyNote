package com.job4me.chord;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ChordDetectionTopologyTest {

    private final Instant base = Instant.now();

    private TopologyTestDriver driver;
    private TestInputTopic<String, NoteEvent> notesIn;
    private TestOutputTopic<String, ChordEvent> chordsOut;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "chord-detection-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:0000");

        StreamsBuilder builder = new StreamsBuilder();
        new ChordDetectionTopology().chordPipeline(builder);
        Topology topology = builder.build();

        driver = new TopologyTestDriver(topology, props, base);
        notesIn = driver.createInputTopic(
                ChordDetectionTopology.NOTE_TOPIC, new StringSerializer(), new JsonSerializer<>());
        chordsOut = driver.createOutputTopic(
                ChordDetectionTopology.CHORD_TOPIC, new StringDeserializer(), new JsonDeserializer<>(ChordEvent.class));
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void groupsNotesStruckTogetherIntoOneChord() {
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 60, 100, base.toEpochMilli()), base);
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 64, 100, base.plusMillis(10).toEpochMilli()), base.plusMillis(10));
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 67, 100, base.plusMillis(20).toEpochMilli()), base.plusMillis(20));

        // suppress withholds the session until stream time passes windowEnd + grace;
        // with no further real notes, only the heartbeat can push it past that point.
        assertThat(chordsOut.isEmpty()).isTrue();

        driver.advanceWallClockTime(Duration.ofMillis(250));

        ChordEvent chord = chordsOut.readValue();
        assertThat(chord.sourceId()).isEqualTo("kbd-1");
        assertThat(chord.notes()).containsExactly(60, 64, 67);
        assertThat(chordsOut.isEmpty()).isTrue();
    }

    @Test
    void singleNoteIsNotAChordEvenAfterFlush() {
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 60, 100, base.toEpochMilli()), base);

        driver.advanceWallClockTime(Duration.ofMillis(250));

        assertThat(chordsOut.isEmpty()).isTrue();
    }

    @Test
    void notesFartherApartThanTheGapAreSeparateChords() {
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 60, 100, base.toEpochMilli()), base);
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 64, 100, base.plusMillis(5).toEpochMilli()), base.plusMillis(5));

        // well past the 50ms inactivity gap: starts a new session, not a merge
        Instant later = base.plusMillis(500);
        Instant lastEvent = later.plusMillis(5);
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 67, 100, later.toEpochMilli()), later);
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 71, 100, lastEvent.toEpochMilli()), lastEvent);

        // the driver's mock wall clock is independent of record timestamps (in
        // production both are the same real clock), so advance it relative to
        // the last event rather than from a fixed offset off `base`.
        driver.advanceWallClockTime(Duration.between(base, lastEvent).plus(Duration.ofMillis(250)));

        ChordEvent first = chordsOut.readValue();
        assertThat(first.notes()).containsExactly(60, 64);
        ChordEvent second = chordsOut.readValue();
        assertThat(second.notes()).containsExactly(67, 71);
        assertThat(chordsOut.isEmpty()).isTrue();
    }
}
