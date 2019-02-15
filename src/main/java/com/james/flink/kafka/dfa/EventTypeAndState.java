package com.james.flink.kafka.dfa;

import com.james.flink.kafka.event.EventType;

/**
 * Simple combination of EventType and State.
 */
public class EventTypeAndState {

	public final EventType eventType;

	public final State state;

	public EventTypeAndState(EventType eventType, State state) {
		this.eventType = eventType;
		this.state = state;
	}
}
