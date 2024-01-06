
package com.concerto.crud.common.bean;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 *
 * @File : com.concerto.crud.common.bean.Bean.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Represents a collection of fields in a database table. This class
 *          encapsulates multiple Field instances, facilitating the organization
 *          and manipulation of database table fields within a broader context.
 * @Version : 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Bean implements Entity {

	@JsonProperty("BeanName")
	private String beanName;

	@JsonProperty("Fields")
	private List<Field> fields;

	
	@Override
	public String getEntityName() {
		return beanName;
	}

	public void setBeanName(String beanName) {
		this.beanName = beanName;
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
		return "Bean [beanName=" + beanName + ", fields=" + fields +  "]";
	}

	@Override
	public boolean isSubBean() {
		return true;
	}
}
