package com.job4me.chord;

import java.util.ArrayList;
import java.util.TreeSet;

/**
 * Accumulates the notes seen within one session window.
 * Plain getters/setters + no-arg constructor so it round-trips through the
 * JSON state store (Kafka Streams persists this between updates and restarts).
 */
public class ChordBuilder {

    private TreeSet<Integer> notes;
    private long startTs;
    private long endTs;

    public ChordBuilder() {
        this.notes = new TreeSet<>();
        this.startTs = Long.MAX_VALUE;
        this.endTs = Long.MIN_VALUE;
    }

    public ChordBuilder add(NoteEvent e) {
        notes.add(e.note());
        startTs = Math.min(startTs, e.timestamp());
        endTs = Math.max(endTs, e.timestamp());
        return this;
    }

    /** Session windows can merge two open sessions; combine their accumulators. */
    public ChordBuilder merge(ChordBuilder other) {
        notes.addAll(other.notes);
        startTs = Math.min(startTs, other.startTs);
        endTs = Math.max(endTs, other.endTs);
        return this;
    }

    public int size() { return notes.size(); }

    public ChordEvent toChord(String sourceId) {
        return new ChordEvent(sourceId, new ArrayList<>(notes), startTs, endTs);
    }

    // --- accessors for JSON (de)serialization ---
    public TreeSet<Integer> getNotes() { return notes; }
    public void setNotes(TreeSet<Integer> notes) { this.notes = notes; }
    public long getStartTs() { return startTs; }
    public void setStartTs(long startTs) { this.startTs = startTs; }
    public long getEndTs() { return endTs; }
    public void setEndTs(long endTs) { this.endTs = endTs; }
}
