package com.concerto.crud.common.util;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;

import com.concerto.crud.common.bean.Bean;
import com.concerto.crud.common.bean.Field;
import com.concerto.crud.common.bean.Module;
import com.concerto.crud.common.constant.AppConstant;
import com.concerto.crud.common.exception.JsonConversionException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.util.JsonToJavaConverter.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Utility class for converting JSON data to Java objects,
 *          particularly instances of the Module class. Reads JSON data from a
 *          specified file path, maps it to Module objects, and provides access
 *          to the resulting module data. Also, manages a map of module names to
 *          their corresponding CommonDAO bean, facilitating the retrieval of
 *          specific CommonDAO implementations for each module.
 * @Version : 1.0
 */

@Configuration
public class JsonToJavaConverter {

	@Autowired
	ApplicationContext applicationContext;

	@Value("${input.json.file.path}")
	private String inputJsonFilePath;

	private static Map<String, Module> moduleMap = new HashMap<>();
	private static Map<String, Object> moduleNameMap = new HashMap<>();
	private static Map<String, List<String>> modulePrimaryfields = new HashMap<>();
	private static Map<String, List<String>> beanPrimaryfields = new HashMap<>();
	private static Map<String, Bean> subBeanMap = new HashMap<>();

	public static Bean subBeanData(String beanName) {
		try {
			if (subBeanMap.containsKey(beanName)) {
				return subBeanMap.get(beanName);
			}
		} catch (Exception e) {
			// Handle other exceptions
			Logging.error(AppConstant.MODULE_RETRIEVING_ERROR, e);
			throw new JsonConversionException(AppConstant.MODULE_DATA_RETRIEVAL_FAILED, e);
		}
		return null;
	}

	/**
	 * Retrieves the Module data based on the provided module name.
	 *
	 * @param moduleName
	 *            The name of the module to retrieve.
	 * @return The Module instance corresponding to the provided module name.
	 * @throws JsonConversionException
	 *             If an error occurs during module retrieval.
	 */
	public static Module moduleData(String moduleName) {
		try {
			if (moduleMap.containsKey(moduleName)) {
				return moduleMap.get(moduleName);
			}
		} catch (Exception e) {
			// Handle other exceptions
			Logging.error(AppConstant.MODULE_RETRIEVING_ERROR, e);
			throw new JsonConversionException(AppConstant.MODULE_DATA_RETRIEVAL_FAILED, e);
		}
		return null;
	}

	/**
	 * Populates the moduleMap with Module instances by reading JSON data from the
	 * specified file path.
	 */
	public void moduleMap() {

		ObjectMapper objectMapper = new ObjectMapper();

		try {
			// Load the input.json file from the classpath

			File file = new File(inputJsonFilePath);

			// Read JSON data from the InputStream and convert to Module array
			Module[] modules = objectMapper.readValue(file, Module[].class);

			// Create a HashMap to store modules using moduleName as key
			for (Module module : modules) {
				processModule(module);
				if(module.getBeans()!=null) {
					processModuleBeans(module);
				}
				
			}
		
		} catch (Exception e) {
			Logging.error(AppConstant.JSON_TO_JAVA_CONVERSION_FAILED, e);
			throw new JsonConversionException(AppConstant.JSON_TO_JAVA_CONVERSION_FAILED, e);
		}
	}

	private void processModule(Module module) {
		moduleMap.put(module.getEntityName(), module);

		List<String> modulePrimaryFields = new ArrayList<>();

		for (Field field : module.getFields()) {
			if (field.isPrimaryKey()) {
				modulePrimaryFields.add(field.getName());
			}
		}

		modulePrimaryfields.put(module.getEntityName(), modulePrimaryFields);
	}

	/**
	 * Processes the main module by adding it to the moduleMap and creating a list
	 * of its primary key fields.
	 *
	 * @param module
	 *            The main module to be processed.
	 */
	private void processModuleBeans(Module module) {
		for (Bean bean : module.getBeans()) {
			subBeanMap.put(bean.getEntityName(), bean);

			List<String> beanPrimaryFields = new ArrayList<>();

			for (Field field : bean.getFields()) {
				if (field.isPrimaryKey()) {
					beanPrimaryFields.add(field.getName());
				}
			}

			beanPrimaryfields.put(bean.getEntityName(), beanPrimaryFields);
		}
	}

	/**
	 * Processes sub-modules (Beans) within the main module. Adds each Bean to the
	 * subBeanMap and creates a list of primary key fields for each Bean.
	 *
	 * @param module
	 *            The main module containing sub-modules (Beans).
	 */
	public static Map<String, Object> getModuleNameMap() {
		return moduleNameMap;
	}

	public static void setModuleNameMap(Map<String, Object> moduleNameMap) {
		JsonToJavaConverter.moduleNameMap = moduleNameMap;
	}

	public static Map<String, Module> getModuleMap() {
		return moduleMap;
	}

	public static void setModuleMap(Map<String, Module> moduleMap) {
		JsonToJavaConverter.moduleMap = moduleMap;
	}

	/**
	 * Gets the primary fields for a given module.
	 *
	 * @param moduleName
	 *            The name of the module.
	 * @return The list of primary fields for the specified module.
	 */
	public static List<String> getPrimaryfields(String moduleName) {
		return modulePrimaryfields.get(moduleName);
	}

	/**
	 * Gets the primary fields for beans.
	 *
	 * @param beanName
	 *            The name of the bean.
	 * @return The list of primary fields for the specified bean.
	 */
	public static List<String> getBeanPrimaryfields(String beanName) {
		return beanPrimaryfields.get(beanName);
	}

}