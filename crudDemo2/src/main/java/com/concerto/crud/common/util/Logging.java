package com.concerto.crud.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.util.Logging.java
 * @Author : Mayur S Wani
 * @Purpose : Logging of error, info, debug
 * @Version : 1.0
 */
public class Logging {

	private Logging() {

	}

	private static final Logger logger = LoggerFactory.getLogger(Logging.class);

	public static void error(String message, Exception pException) {
		logger.error(message, pException);

	}
	public static void error(String message) {
		logger.error(message);

	}
	public static void info(String pxml) {
		logger.info(pxml);
	}

	public static void printInfo(String pMessage) {
		logger.info(pMessage);
	}

	public static void printDebug(String pMessage) {
		logger.debug(pMessage);
	}
}
