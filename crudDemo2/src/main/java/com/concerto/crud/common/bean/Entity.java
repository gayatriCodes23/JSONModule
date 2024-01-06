package com.concerto.crud.common.bean;

import java.util.List;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 *
 * @File : com.concerto.crud.common.bean.Entity.java
 * @Author : Gayatri Hande
 * @AddedDate : 26th December 2023
 * @Purpose : Interface representing an entity in the database schema. An entity
 *          defines the structure of a database table and encapsulates
 *          information such as the entity name, list of fields, and whether it
 *          is considered a sub-bean. Entities are used to model the underlying
 *          data structure and support interactions with the database. This
 *          interface provides methods for retrieving the entity name, list of
 *          fields, and determining whether it is a sub-bean.
 * @Version : 1.0
 */

public interface Entity {

	String getEntityName();

	List<Field> getFields();

	boolean isSubBean();
}
