package com.example.chord;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.time.Duration;

/**
 * Passes real note records through untouched. On a wall-clock schedule it also
 * forwards a synthetic HEARTBEAT record so the downstream suppress stage keeps
 * observing an advancing timestamp during silence -- otherwise the last chord
 * of a burst sits buffered forever, since suppress only re-checks its buffer
 * when a new record reaches it, and session windows only advance on data.
 *
 * The heartbeat's own key ("__heartbeat__") ends up as a same-named, always
 * sub-chord-size session that gets filtered out downstream like any other
 * single-note session; it exists only to nudge stream time forward.
 */
public class HeartbeatInjector implements Processor<String, NoteEvent, String, NoteEvent> {

    static final String HEARTBEAT_KEY = "__heartbeat__";
    static final String HEARTBEAT_TYPE = "HEARTBEAT";

    private final Duration interval;
    private ProcessorContext<String, NoteEvent> ctx;

    public HeartbeatInjector(Duration interval) {
        this.interval = interval;
    }

    @Override
    public void init(ProcessorContext<String, NoteEvent> context) {
        this.ctx = context;
        context.schedule(interval, PunctuationType.WALL_CLOCK_TIME, this::heartbeat);
    }

    @Override
    public void process(Record<String, NoteEvent> record) {
        ctx.forward(record);
    }

    private void heartbeat(long wallClockTime) {
        NoteEvent tick = new NoteEvent(HEARTBEAT_KEY, HEARTBEAT_TYPE, 0, 0, wallClockTime);
        ctx.forward(new Record<>(HEARTBEAT_KEY, tick, wallClockTime));
    }
}
