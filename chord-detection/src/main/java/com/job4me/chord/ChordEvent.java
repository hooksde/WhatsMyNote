package com.job4me.chord;

import java.util.List;

/** A detected chord: the set of notes struck together, and the time span they spanned. */
public record ChordEvent(String sourceId, List<Integer> notes, long startTs, long endTs) {}
