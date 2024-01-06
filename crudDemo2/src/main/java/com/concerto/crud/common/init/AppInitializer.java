package com.concerto.crud.common.init;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.concerto.crud.common.util.JsonToJavaConverter;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.init.AppInitializer.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Initializes and processes data on application startup using the
 *          JsonToJavaConverter.
 * @Version : 1.0
 */

@Component
public class AppInitializer implements CommandLineRunner {

	/* Field */
	private final JsonToJavaConverter jsonToJavaConverter;

	/* Constructor */

	@Autowired
	public AppInitializer(JsonToJavaConverter jsonToJavaConverter) {
		this.jsonToJavaConverter = jsonToJavaConverter;
	}

	/* CommandLineRunner run Method */

	@Override
	public void run(String... args) throws Exception {
		jsonToJavaConverter.moduleMap();

	}

}
