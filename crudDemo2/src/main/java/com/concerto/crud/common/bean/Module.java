package com.concerto.crud.common.bean;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 *
 * @File : com.concerto.crud.common.bean.Module.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Represents a module in a software system, encapsulating
 *          information such as the module name and a list of associated fields.
 *          This class is utilized to model and manage the structure of modules
 *          within the application. The module may contain various fields, each
 *          represented by the {@link Field} class. Additionally, it provides a
 *          method to extract module parameters for further processing or
 *          interaction with other components.
 * @Version : 1.0
 */

public class Module implements Entity {

	@JsonProperty("ModuleName")
	private String moduleName;

	@JsonProperty("Fields")
	private List<Field> fields;

	@JsonProperty("Beans")
	private List<Bean> beans;

	public String getEntityName() {
		return moduleName;
	}

	public List<Bean> getBeans() {
		return beans;
	}

	public void setBeans(List<Bean> beans) {
		this.beans = beans;
	}

	public void setModuleName(String moduleName) {
		this.moduleName = moduleName;
	}

	@Override
	public List<Field> getFields() {
		return fields;
	}

	public void setFields(List<Field> fields) {
		this.fields = fields;
	}

	@Override
	public String toString() {
		return "Module [moduleName=" + moduleName + ", fields=" + fields + ", beans=" + beans + "]";
	}

	public Map<String, Object> getParam(Map<String, Object> params) {
		params.put("moduleName", this.moduleName);
		return params;
	}

	@Override
	public boolean isSubBean() {
		return false;
	}

}