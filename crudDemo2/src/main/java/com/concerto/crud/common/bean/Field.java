
package com.concerto.crud.common.bean;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 *
 * @File : com.concerto.crud.common.bean.Field.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Represents a field in a database table, encapsulating information
 *          such as the field name, data type, and optional validation rules.
 *          This class is used to define the structure of database tables and
 *          facilitates interaction with the underlying data model.
 *          Additionally, it supports the identification of primary key fields
 *          for database schema design.
 * @Version : 1.0
 */
public class Field {

	private String name;

	private String type;

	private boolean primaryKey = false;

	private Validation validation;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Validation getValidation() {
		return validation;
	}

	public void setValidation(Validation validation) {
		this.validation = validation;
	}

	@Override
	public String toString() {
		return "Field [name=" + name + ", type=" + type + ", validation=" + validation + "]";
	}

	public boolean isPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(boolean primaryKey) {
		this.primaryKey = primaryKey;
	}

}