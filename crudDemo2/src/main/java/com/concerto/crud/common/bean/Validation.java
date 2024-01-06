package com.concerto.crud.common.bean;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 *
 * @File : com.concerto.crud.common.bean.Validation.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Represents validation rules for a field, defining constraints such
 *          as pattern matching, maximum length, minimum, and maximum values.
 *          Instances of this class are used to specify and enforce validation
 *          criteria on fields within the application, ensuring data integrity
 *          and adherence to business rules.
 * @Version : 1.0
 */

public class Validation {

	private String pattern;

	private int minimum;

	private int maximum;

	public String getPattern() {
		return pattern;
	}

	public void setPattern(String pattern) {
		this.pattern = pattern;
	}

	public int getMinimum() {
		return minimum;
	}

	public void setMinimum(int minimum) {
		this.minimum = minimum;
	}

	public int getMaximum() {
		return maximum;
	}

	public void setMaximum(int maximum) {
		this.maximum = maximum;
	}

	@Override
	public String toString() {
		return "Validation [pattern=" + pattern + ", minimum=" + minimum + ", maximum=" + maximum + "]";
	}

}
