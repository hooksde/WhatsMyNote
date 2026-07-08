package com.job4me.sink;

import java.util.List;

public record ChordEvent(String sourceId, List<Integer> notes, long startTs, long endTs) {}
