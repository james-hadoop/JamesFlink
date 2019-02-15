package com.james.flink.utils;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Types the parameters of managed with {@link RequiredParameters} can take.
 *
 * <p>Name maps directly to the corresponding Java type.
 */
@PublicEvolving
public enum OptionType {
	INTEGER,
	LONG,
	DOUBLE,
	FLOAT,
	BOOLEAN,
	STRING
}
