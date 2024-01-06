package com.concerto.crud.common.exception;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.exception.DatabaseConnectionException.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Represents a custom exception to be used for handling
 *          database-specific errors.
 * @Version : 1.0
 */

public class DataSourceException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public DataSourceException(String message) {
		super(message);
	}

	public DataSourceException(String message, Throwable cause) {
		super(message, cause);
	}
}