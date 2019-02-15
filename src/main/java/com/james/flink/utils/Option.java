package com.james.flink.utils;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Internal representation of a parameter passed to a user defined function.
 */
@PublicEvolving
public class Option {

	private String longName;
	private String shortName;

	private String defaultValue;
	private Set<String> choices;

	private String helpText;
	private OptionType type = OptionType.STRING;

	public Option(String name) {
		this.longName = name;
		this.choices = new HashSet<>();
	}

	/**
	 * Define an alternative / short name of the parameter.
	 * Only one alternative per parameter is allowed.
	 *
	 * @param shortName - short version of the parameter name
	 * @return the updated Option
	 */
	public Option alt(String shortName) {
		this.shortName = shortName;
		return this;
	}

	/**
	 * Define the type of the Option.
	 *
	 * @param type - the type which the value of the Option can be casted to.
	 * @return the updated Option
	 */
	public Option type(OptionType type) {
		this.type = type;
		return this;
	}

	/**
	 * Define a default value for the option.
	 *
	 * @param defaultValue - the default value
	 * @return the updated Option
	 * @throws RequiredParametersException if the list of possible values for the parameter is not empty and the default
	 *                                     value passed is not in the list.
	 */
	public Option defaultValue(String defaultValue) throws RequiredParametersException {
		if (this.choices.isEmpty()) {
			return this.setDefaultValue(defaultValue);
		} else {
			if (this.choices.contains(defaultValue)) {
				return this.setDefaultValue(defaultValue);
			} else {
				throw new RequiredParametersException("Default value " + defaultValue +
						" is not in the list of valid values for option " + this.longName);
			}
		}
	}

	/**
	 * Restrict the list of possible values of the parameter.
	 *
	 * @param choices - the allowed values of the parameter.
	 * @return the updated Option
	 */
	public Option choices(String... choices) throws RequiredParametersException {
		if (this.defaultValue != null) {
			if (Arrays.asList(choices).contains(defaultValue)) {
				Collections.addAll(this.choices, choices);
			} else {
				throw new RequiredParametersException("Valid values for option " + this.longName +
						" do not contain defined default value " + defaultValue);
			}
		} else {
			Collections.addAll(this.choices, choices);
		}
		return this;
	}

	/**
	 * Add a help text, explaining the parameter.
	 *
	 * @param helpText - the help text.
	 * @return the updated Option
	 */
	public Option help(String helpText) {
		this.helpText = helpText;
		return this;
	}

	public String getName() {
		return this.longName;
	}

	public boolean hasAlt() {
		return this.shortName != null;
	}

	public boolean hasType() {
		return this.type != null;
	}

	public OptionType getType() {
		return this.type;
	}

	public String getAlt() {
		return this.shortName;
	}

	public String getHelpText() {
		return this.helpText;
	}

	public Set<String> getChoices() {
		return this.choices;
	}

	public boolean hasDefaultValue() {
		return this.defaultValue != null;
	}

	public String getDefaultValue() {
		return this.defaultValue;
	}

	private Option setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
		return this;
	}

	public boolean isCastableToDefinedType(String value) {
		switch (this.type) {
			case INTEGER:
				try {
					Integer.parseInt(value);
				} catch (NumberFormatException nfe) {
					return false;
				}
				return true;
			case LONG:
				try {
					Long.parseLong(value);
				} catch (NumberFormatException nfe) {
					return false;
				}
				return true;
			case FLOAT:
				try {
					Float.parseFloat(value);
				} catch (NumberFormatException nfe) {
					return false;
				}
				return true;
			case DOUBLE:
				try {
					Double.parseDouble(value);
				} catch (NumberFormatException nfe) {
					return false;
				}
				return true;
			case BOOLEAN:
				return Objects.equals(value, "true") || Objects.equals(value, "false");
			case STRING:
				return true;
		}
		throw new IllegalStateException("Invalid value for OptionType " + this.type + " for option " + this.longName);
	}

}
