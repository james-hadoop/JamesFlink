package com.james.flink.utils;

import org.apache.flink.annotation.PublicEvolving;

import java.util.LinkedList;
import java.util.List;

/**
 * Exception which is thrown if validation of {@link RequiredParameters} fails.
 */
@PublicEvolving
public class RequiredParametersException extends Exception {

	private List<String> missingArguments;

	public RequiredParametersException() {
		super();
	}

	public RequiredParametersException(String message, List<String> missingArguments) {
		super(message);
		this.missingArguments = missingArguments;
	}

	public RequiredParametersException(String message) {
		super(message);
	}

	public RequiredParametersException(String message, Throwable cause) {
		super(message, cause);
	}

	public RequiredParametersException(Throwable cause) {
		super(cause);
	}

	public List<String> getMissingArguments() {
		if (missingArguments == null) {
			return new LinkedList<>();
		} else {
			return this.missingArguments;
		}
	}
}
