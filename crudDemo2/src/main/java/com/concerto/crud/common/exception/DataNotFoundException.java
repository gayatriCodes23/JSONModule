package com.concerto.crud.common.exception;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.exception.CustomException.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Represents a custom exception to be used for handling
 *          application-specific errors.
 * @Version : 1.0
 */

public class DataNotFoundException extends Exception {

	private static final long serialVersionUID = 1L;

	public DataNotFoundException(String message) {
		super(message);
	}

	public DataNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

	public DataNotFoundException(String commonModuleError, String string) {
	}

}
