package com.job4me.chord;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

/**
 * Interval-overlap chord detection. Alternative to ChordDetectionTopology;
 * enable with the "overlap" Spring profile (the session-window version uses "session").
 */
@Configuration
@Profile("overlap")
@EnableKafkaStreams
public class ChordProcessorTopology {

    public static final String NOTE_TOPIC  = "note-events";
    public static final String CHORD_TOPIC = "chord-events";
    public static final String STORE       = "held-notes-store";

    @Bean
    public KStream<String, NoteEvent> overlapPipeline(StreamsBuilder builder) {
        JsonSerde<NoteEvent>  noteSerde  = new JsonSerde<>(NoteEvent.class).ignoreTypeHeaders();
        JsonSerde<HeldState>  heldSerde  = new JsonSerde<>(HeldState.class).ignoreTypeHeaders();
        JsonSerde<ChordEvent> chordSerde = new JsonSerde<>(ChordEvent.class).ignoreTypeHeaders();

        // the processor needs a persistent store to survive restarts and rebalances
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE), Serdes.String(), heldSerde));

        KStream<String, NoteEvent> notes =
                builder.stream(NOTE_TOPIC, Consumed.with(Serdes.String(), noteSerde));

        notes
            .<String, ChordEvent>process(ChordProcessor::new, STORE)
            .to(CHORD_TOPIC, Produced.with(Serdes.String(), chordSerde));

        return notes;
    }
}
