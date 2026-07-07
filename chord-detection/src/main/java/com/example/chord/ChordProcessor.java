package com.example.chord;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Detects chords by tracking which notes are actually held at the same time,
 * rather than just "struck close together."
 *
 * State machine per keyboard (keyed by sourceId):
 *   NOTE_ON  -> add note to the held set. If the set is now a chord (>=2),
 *               re-arm: a new or larger chord may need to be emitted.
 *   NOTE_OFF -> if the set was a chord and we haven't emitted it yet, emit the
 *               notes that were sounding together, then mark it emitted.
 *               Remove the note; if the set drops below 2, the cluster is over.
 *
 * Net effect: holding C+E+G and releasing any one emits {C, E, G} once. Adding
 * a 4th note while 3 are held re-arms so the larger chord can emit on release.
 */
public class ChordProcessor implements Processor<String, NoteEvent, String, ChordEvent> {

    private static final int MIN_CHORD = 2;
    private static final String STORE = "held-notes-store";

    // A dropped NoteOff leaves a note "stuck" held forever. Anything held past
    // this long is implausible (no sustain pedal holds a note this long) and
    // gets evicted so it can't keep inflating chords indefinitely.
    private static final long MAX_HOLD_MILLIS = Duration.ofSeconds(30).toMillis();
    private static final Duration EVICTION_CHECK_INTERVAL = Duration.ofSeconds(5);

    private ProcessorContext<String, ChordEvent> ctx;
    private KeyValueStore<String, HeldState> store;

    @Override
    public void init(ProcessorContext<String, ChordEvent> context) {
        this.ctx = context;
        this.store = context.getStateStore(STORE);
        context.schedule(EVICTION_CHECK_INTERVAL, PunctuationType.WALL_CLOCK_TIME, this::evictStuckNotes);
    }

    private void evictStuckNotes(long wallClockTime) {
        List<KeyValue<String, HeldState>> updated = new ArrayList<>();
        try (KeyValueIterator<String, HeldState> all = store.all()) {
            while (all.hasNext()) {
                KeyValue<String, HeldState> entry = all.next();
                HeldState state = entry.value;
                boolean evicted = state.getHeld().values()
                        .removeIf(onsetTs -> wallClockTime - onsetTs > MAX_HOLD_MILLIS);
                if (evicted) {
                    if (state.getHeld().size() < MIN_CHORD) {
                        state.setPeakEmitted(false);
                    }
                    updated.add(entry);
                }
            }
        }
        updated.forEach(kv -> store.put(kv.key, kv.value));
    }

    @Override
    public void process(Record<String, NoteEvent> record) {
        String source = record.key();
        NoteEvent e = record.value();
        if (source == null || e == null) return;

        HeldState state = store.get(source);
        if (state == null) state = new HeldState();

        if ("NOTE_ON".equals(e.type()) && e.velocity() > 0) {
            state.getHeld().put(e.note(), e.timestamp());
            if (state.getHeld().size() >= MIN_CHORD) {
                state.setPeakEmitted(false);            // chord formed or grew: re-arm
            }
        } else if ("NOTE_OFF".equals(e.type())) {
            if (state.getHeld().size() >= MIN_CHORD && !state.isPeakEmitted()) {
                // the notes sounding right now (including the one being released) are a chord
                List<Integer> notes = new ArrayList<>(new TreeSet<>(state.getHeld().keySet()));
                long startTs = state.getHeld().values().stream().min(Long::compareTo).orElse(e.timestamp());
                ctx.forward(record.withValue(new ChordEvent(source, notes, startTs, e.timestamp())));
                state.setPeakEmitted(true);
            }
            state.getHeld().remove(e.note());
            if (state.getHeld().size() < MIN_CHORD) {
                state.setPeakEmitted(false);            // cluster over, ready for the next one
            }
        }

        store.put(source, state);
    }
}
