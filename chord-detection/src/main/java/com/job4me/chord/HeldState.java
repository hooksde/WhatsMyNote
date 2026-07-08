package com.job4me.chord;

import java.util.HashMap;
import java.util.Map;

/**
 * Per-keyboard state for the interval-overlap detector.
 * Tracks which notes are currently held (note -> onset timestamp) plus whether
 * we've already emitted a chord for the current overlapping cluster.
 *
 * Plain getters/setters + no-arg constructor so it round-trips through the
 * JSON-backed state store.
 */
public class HeldState {

    private Map<Integer, Long> held;
    private boolean peakEmitted;

    public HeldState() {
        this.held = new HashMap<>();
        this.peakEmitted = false;
    }

    public Map<Integer, Long> getHeld() { return held; }
    public void setHeld(Map<Integer, Long> held) { this.held = held; }
    public boolean isPeakEmitted() { return peakEmitted; }
    public void setPeakEmitted(boolean peakEmitted) { this.peakEmitted = peakEmitted; }
}
