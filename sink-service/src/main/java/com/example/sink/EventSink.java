package com.example.sink;

/** Storage backend for note and chord events. Implemented per cloud DB. */
public interface EventSink {
    void write(NoteEvent note);
    void write(ChordEvent chord);
}
