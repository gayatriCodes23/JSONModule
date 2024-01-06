package com.concerto.crud.common.exception;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.exception.JsonConversionException.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Represents a custom exception to be used for handling
 *          application-specific errors.
 * @Version : 1.0
 */


public class JsonConversionException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public JsonConversionException(String message) {
		super(message);
	}

	public JsonConversionException(String message, Throwable cause) {
		super(message, cause);
	}
}
