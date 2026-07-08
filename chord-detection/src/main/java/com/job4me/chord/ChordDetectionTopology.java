package com.job4me.chord;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

/**
 * Turns the raw note-events stream into chord-events.
 *
 * A chord here means "two or more notes struck close together on the same
 * keyboard." We model that with a SESSION WINDOW: notes arriving within
 * CHORD_GAP of each other land in the same session; a quiet gap closes it.
 */
@Configuration
@Profile("session")
@EnableKafkaStreams
public class ChordDetectionTopology {

    public static final String NOTE_TOPIC  = "note-events";
    public static final String CHORD_TOPIC = "chord-events";

    // Notes pressed within this gap are treated as one chord. Tune to taste:
    // ~50ms catches block chords and strums without merging a fast melody line.
    private static final Duration CHORD_GAP = Duration.ofMillis(50);
    private static final Duration GRACE     = Duration.ofMillis(50);

    // How often the heartbeat nudges stream time forward during silence, so the
    // last chord of a burst is flushed promptly instead of waiting for the next
    // note (see HeartbeatInjector).
    private static final Duration FLUSH_CHECK_INTERVAL = Duration.ofMillis(200);

    @Bean
    public KStream<String, NoteEvent> chordPipeline(StreamsBuilder builder) {
        JsonSerde<NoteEvent>   noteSerde  = new JsonSerde<>(NoteEvent.class).ignoreTypeHeaders();
        JsonSerde<ChordBuilder> accSerde  = new JsonSerde<>(ChordBuilder.class).ignoreTypeHeaders();
        JsonSerde<ChordEvent>  chordSerde = new JsonSerde<>(ChordEvent.class).ignoreTypeHeaders();

        KStream<String, NoteEvent> notes =
                builder.stream(NOTE_TOPIC, Consumed.with(Serdes.String(), noteSerde));

        notes
            // a chord is built from PRESSED notes; releases don't compose it
            .filter((sourceId, e) -> "NOTE_ON".equals(e.type()) && e.velocity() > 0)
            // keep the session/suppress stage seeing advancing timestamps even
            // when notes stop arriving, so the last chord of a burst still flushes
            .process(() -> new HeartbeatInjector(FLUSH_CHECK_INTERVAL))
            // key is the sourceId, so each keyboard is grouped independently
            .groupByKey(Grouped.with(Serdes.String(), noteSerde))
            // collapse a burst of near-simultaneous notes into one session
            .windowedBy(SessionWindows.ofInactivityGapAndGrace(CHORD_GAP, GRACE))
            .aggregate(
                    ChordBuilder::new,                          // initializer
                    // heartbeats only advance stream time; they never become notes
                    (sourceId, e, acc) -> HeartbeatInjector.HEARTBEAT_TYPE.equals(e.type()) ? acc : acc.add(e),
                    (sourceId, a, b) -> a.merge(b),              // merge two sessions
                    Materialized.with(Serdes.String(), accSerde))
            // emit each session exactly once, after it closes (instead of on every update)
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            // 2+ notes = chord; a single note in the window is just a note, drop it
            .filter((windowedKey, acc) -> acc != null && acc.size() >= 2)
            .map((windowedKey, acc) ->
                    KeyValue.pair(windowedKey.key(), acc.toChord(windowedKey.key())))
            .to(CHORD_TOPIC, Produced.with(Serdes.String(), chordSerde));

        return notes;
    }
}
