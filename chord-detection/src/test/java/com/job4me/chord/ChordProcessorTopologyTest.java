package com.job4me.chord;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ChordProcessorTopologyTest {

    private final Instant base = Instant.now();

    private TopologyTestDriver driver;
    private TestInputTopic<String, NoteEvent> notesIn;
    private TestOutputTopic<String, ChordEvent> chordsOut;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "chord-overlap-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:0000");

        StreamsBuilder builder = new StreamsBuilder();
        new ChordProcessorTopology().overlapPipeline(builder);
        Topology topology = builder.build();

        driver = new TopologyTestDriver(topology, props, base);
        notesIn = driver.createInputTopic(
                ChordProcessorTopology.NOTE_TOPIC, new StringSerializer(), new JsonSerializer<>());
        chordsOut = driver.createOutputTopic(
                ChordProcessorTopology.CHORD_TOPIC, new StringDeserializer(), new JsonDeserializer<>(ChordEvent.class));
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void emitsChordOnceWhenAHeldNoteIsReleased() {
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 60, 100, base.toEpochMilli()), base);
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 64, 100, base.plusMillis(5).toEpochMilli()), base.plusMillis(5));
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 67, 100, base.plusMillis(10).toEpochMilli()), base.plusMillis(10));

        Instant release = base.plusMillis(500);
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_OFF", 60, 0, release.toEpochMilli()), release);

        ChordEvent chord = chordsOut.readValue();
        assertThat(chord.notes()).containsExactly(60, 64, 67);
        assertThat(chordsOut.isEmpty()).isTrue(); // emitted once, not per remaining held note

        Instant secondRelease = release.plusMillis(10);
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_OFF", 64, 0, secondRelease.toEpochMilli()), secondRelease);
        assertThat(chordsOut.isEmpty()).isTrue();
    }

    @Test
    void evictsANoteHeldImplausiblyLong() {
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 60, 100, base.toEpochMilli()), base);

        // past MAX_HOLD_MILLIS (30s), crossing several 5s eviction-check ticks
        driver.advanceWallClockTime(Duration.ofSeconds(36));

        KeyValueStore<String, HeldState> store = driver.getKeyValueStore("held-notes-store");
        HeldState state = store.get("kbd-1");
        assertThat(state.getHeld()).isEmpty();
    }

    @Test
    void evictedNoteNoLongerCountsTowardAChord() {
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 60, 100, base.toEpochMilli()), base);

        driver.advanceWallClockTime(Duration.ofSeconds(36)); // evicts note 60

        Instant later = base.plusSeconds(36).plusMillis(10);
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 64, 100, later.toEpochMilli()), later);
        Instant evenLater = later.plusMillis(5);
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_ON", 67, 100, evenLater.toEpochMilli()), evenLater);
        Instant release = evenLater.plusMillis(100);
        notesIn.pipeInput("kbd-1", new NoteEvent("kbd-1", "NOTE_OFF", 64, 0, release.toEpochMilli()), release);

        ChordEvent chord = chordsOut.readValue();
        // if note 60 hadn't been evicted, this would be [60, 64, 67]
        assertThat(chord.notes()).containsExactly(64, 67);
    }
}
