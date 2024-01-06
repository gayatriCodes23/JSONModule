package com.concerto.crud.common.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.concerto.crud.common.bean.Bean;
import com.concerto.crud.common.bean.Field;
import com.concerto.crud.common.bean.Module;
import com.concerto.crud.common.constant.AppConstant;
import com.concerto.crud.common.exception.DataSourceException;
import com.concerto.crud.common.util.JsonToJavaConverter;
import com.concerto.crud.common.util.Logging;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.dao.CommonDAOImpl.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Implements methods for executing create, update, read, delete, and
 *          other operations on a specified module in the database. This class
 *          utilizes JDBC and Spring JDBC Template for database interactions. It
 *          also handles data insertion into a temporary table for further
 *          processing.
 * @Version : 1.0
 */

@Repository
public class CommonDAOImpl implements CommonDAO {

	private JdbcTemplate jdbcTemplate;

	private DataSource dataSource;

	@Autowired
	public CommonDAOImpl(DataSource dataSource, JdbcTemplate jdbcTemplate) {
		if (dataSource == null) {
			throw new DataSourceException(AppConstant.DATASOURCE_NULL);
		}
		this.dataSource = dataSource;
		this.jdbcTemplate = jdbcTemplate;
	}

	/**
	 * Executes a database query to retrieve data based on the provided field name,
	 * value, and module.
	 *
	 * @param fieldName
	 *            The name of the field to filter by.
	 * @param value
	 *            The value to filter the field by.
	 * @param module
	 *            The module for which data is retrieved.
	 * @return A map containing the retrieved data.
	 */
	@Override
	public List<Map<String, Object>> executeGetData(String fieldName, Object value, Module module) {
		Map<String, Object> result = new HashMap<>();
		String tableName = module.getEntityName();
		List<Field> fields = module.getFields();
		List<Bean> beans = module.getBeans();
		String columns = fields.stream().map(Field::getName).collect(Collectors.joining(AppConstant.COMMA));
		String tableNameWithSuffix = tableName + AppConstant.MASTER_TABLE_SUFFIX;
		String selectQuery = String.format(AppConstant.SELECT_QUERY, columns, tableNameWithSuffix, fieldName);
		List<Map<String, Object>> response = new ArrayList<>();
		try {
			List<Map<String, Object>> queryResult = jdbcTemplate.queryForList(selectQuery, value);
			if (!queryResult.isEmpty()) {

				for (Map<String, Object> iterateData : queryResult) {
					if (beans != null) {
						processAllBeanData(iterateData, beans, response);
					}
					response.add(iterateData);
				}

			}
		} catch (Exception e) {
			Logging.error(AppConstant.FETCH_PROCESS_ERROR, e);
			result.put(AppConstant.COMMON_MODULE_ERROR, AppConstant.DATA_READING_ERROR);
		}

		return response;
	}

	/**
	 * Executes a database query to retrieve all data for the specified module.
	 *
	 * @param module
	 *            The module for which all data is retrieved.
	 * @return A list containing maps of all retrieved data.
	 */
	@Override
	public List<Map<String, Object>> executeGetAllData(Module module) {
		String tableName = module.getEntityName();
		List<Bean> beans = module.getBeans();
		String tableNameWithSuffix = tableName + AppConstant.MASTER_TABLE_SUFFIX;
		String selectQuery = String.format(AppConstant.SELECT_ALL_QUERY, tableNameWithSuffix);
		List<Map<String, Object>> response = new ArrayList<>();

		try {
			List<Map<String, Object>> data = jdbcTemplate.queryForList(selectQuery);

			for (Map<String, Object> iterateData : data) {
				if (beans != null) {
					processAllBeanData(iterateData, beans, response);
				}
				response.add(iterateData);
			}
		} catch (Exception e) {
			Logging.error(AppConstant.FETCH_PROCESS_ERROR, e);
			Map<String, Object> errorMap = new HashMap<>();
			errorMap.put(AppConstant.COMMON_MODULE_ERROR, AppConstant.DATA_READING_ERROR);
			response.add(errorMap);
		}

		return response;
	}

	/**
	 * Processes data for all beans associated with a given module. For each bean,
	 * constructs a SQL query to select data from the corresponding database table
	 * based on the bean's fields and their values in the provided 'iterateData'
	 * map. The result is added to the original data map under the bean's entity
	 * name. Utilizes the 'processField' method to build the WHERE clause of the SQL
	 * query.
	 *
	 * @param iterateData
	 *            The map of data for the module.
	 * @param beans
	 *            List of 'Bean' objects associated with the module.
	 * @param response
	 *            List to which processed data is added.
	 */
	private void processAllBeanData(Map<String, Object> iterateData, List<Bean> beans,
			List<Map<String, Object>> response) {
		for (Bean bean : beans) {
			if (bean == null) {
				continue;
			}

			String beanTableName = bean.getEntityName() + AppConstant.MASTER_TABLE_SUFFIX;
			List<Field> fields = bean.getFields();
			StringBuilder whereClause = new StringBuilder();
			List<Object> parameterValues = new ArrayList<>();

			for (Field field : fields) {
				processField(iterateData, whereClause, parameterValues, field);
			}

			if (whereClause.length() > 0) {
				Object[] parametersArray = parameterValues.toArray();
				String selectBeanQuery = String.format(AppConstant.SELECT_QUERY1, beanTableName, whereClause);
				List<Map<String, Object>> result = jdbcTemplate.queryForList(selectBeanQuery, parametersArray);
				iterateData.put(bean.getEntityName(), result);
				response.add(iterateData);
			}
		}
	}

	/**
	 * Processes a field of a bean to build a WHERE clause for a SQL query. If the
	 * field is a primary key, extracts the field name and value from the
	 * 'iterateData' map, appends them to the WHERE clause, and adds the value to
	 * the parameter values list. This method is used by 'processAllBeanData' to
	 * construct the WHERE clause for selecting data from the associated table of
	 * the bean.
	 *
	 * @param iterateData
	 *            The map of data for the module.
	 * @param whereClause
	 *            StringBuilder representing the WHERE clause of the SQL query.
	 * @param parameterValues
	 *            List to which parameter values for the SQL query are added.
	 * @param field
	 *            The 'Field' object representing the field to process.
	 */
	private void processField(Map<String, Object> iterateData, StringBuilder whereClause, List<Object> parameterValues,
			Field field) {
		if (field.isPrimaryKey()) {
			String primaryKeyFieldName = field.getName();

			if (iterateData.containsKey(primaryKeyFieldName)) {
				Object primaryKeyValue = iterateData.get(primaryKeyFieldName);

				if (primaryKeyValue != null && whereClause.length() > 0) {
					whereClause.append(AppConstant.AND);
				}

				whereClause.append(primaryKeyFieldName).append(AppConstant.EQUAL_QUERY_PARAM);
				parameterValues.add(primaryKeyValue);
			}
		}
	}

	/**
	 * Adds data to the master table in the database for the specified module.
	 *
	 * @param input
	 *            The input data to be added to the master table.
	 * @param module
	 *            The module for which data is added.
	 * @return A boolean indicating whether the data addition was successful.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean addToMaster(Map<String, Object> input, Module module, String action) {
		boolean response = false;
		String tableName = module.getEntityName();
		List<Field> fields = module.getFields();

		List<Bean> beans = module.getBeans();
		Map<String, Object> dataMap = (Map<String, Object>) input.get(AppConstant.PARENT_DATA);
		String request = dataMap.get(AppConstant.REQUEST).toString();
		String setClause = fields.stream().map(Field::getName).collect(Collectors.joining(AppConstant.COMMA));
		if (AppConstant.ADD.equalsIgnoreCase(request)) {
			setClause += AppConstant.COMMA + AppConstant.ADDED_BY + AppConstant.COMMA + AppConstant.ADDED_DATE_TIME
					+ AppConstant.COMMA + AppConstant.APPROVE_BY + AppConstant.COMMA + AppConstant.APPROVE_DATE_TIME;

			dataMap.put(AppConstant.APPROVE_BY, "superuser");
			dataMap.put(AppConstant.APPROVE_DATE_TIME, new Date());
		}

		String[] columnNames = setClause.split(AppConstant.COMMA_SPLIT);
		String insertQuery = AppConstant.INSERT + tableName + AppConstant.MASTER_TABLE_SUFFIX + AppConstant.OPEN_BRACKET
				+ setClause + AppConstant.VALUES
				+ String.join(AppConstant.COMMA, Collections.nCopies(fields.size() + 4, AppConstant.QUERY_PARAM))
				+ AppConstant.CLOSE_BRACKET;

		try (Connection connection = dataSource.getConnection();
				PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
			connection.setAutoCommit(false);

			setParameters(preparedStatement, dataMap, columnNames);
			int rowsAffected = preparedStatement.executeUpdate();

			if (rowsAffected > 0 && beans != null) {
				response = processAddToMasterForBeans(input, connection, beans);

				if (response) {
					connection.commit();
				} else {
					connection.rollback();
				}
			} else if (rowsAffected > 0 && beans == null) {
				connection.commit();
				response = true;
			} else {
				connection.rollback();
			}
		} catch (Exception e) {
			Logging.error(AppConstant.ADD_MASTER_PROCESS_ERROR, e);
			return response;
		}
		return response;
	}

	/**
	 * Processes the addition of data to the child master table for a specified bean
	 * and connection.
	 *
	 * @param input
	 *            The input data to be added to the child master table.
	 * @param bean
	 *            The bean for which data is added.
	 * @param connection
	 *            The database connection.
	 * @return A boolean indicating whether the data addition was successful.
	 */
	@SuppressWarnings("unchecked")
	private boolean processAddToMasterForBeans(Map<String, Object> input, Connection connection, List<Bean> beans) {
		boolean response = false;
		for (Bean bean : beans) {
			List<Map<String, Object>> subBeanDataMap = (List<Map<String, Object>>) input.get(AppConstant.BEAN_DATA);
			if (subBeanDataMap != null) {
				for (int i = 0; i < subBeanDataMap.size(); i++) {
					Map<String, Object> beanMap = subBeanDataMap.get(i);
					boolean result = addToChildMaster(beanMap, bean, connection);
					if (result) {
						response = true;
					}
				}
			}
		}
		return response;
	}

	/**
	 * Adds data to the child master table in the database for a specified bean.
	 *
	 * @param input
	 *            The input data to be added to the child master table.
	 * @param bean
	 *            The bean for which data is added.
	 * @param connection
	 *            The database connection.
	 * @return A boolean indicating whether the data addition was successful.
	 */
	private boolean addToChildMaster(Map<String, Object> input, Bean bean, Connection connection) {
		boolean response = false;
		String tableName = bean.getEntityName();
		List<Field> fields = bean.getFields();
		String request = input.get(AppConstant.REQUEST).toString();
		String setClause = fields.stream().map(Field::getName).collect(Collectors.joining(AppConstant.COMMA));
		if (AppConstant.ADD.equalsIgnoreCase(request)) {
			setClause += AppConstant.COMMA + AppConstant.ADDED_BY + AppConstant.COMMA + AppConstant.ADDED_DATE_TIME
					+ AppConstant.COMMA + AppConstant.APPROVE_BY + AppConstant.COMMA + AppConstant.APPROVE_DATE_TIME;
			input.put(AppConstant.APPROVE_BY, "superuser");
			input.put(AppConstant.APPROVE_DATE_TIME, new Date());
		}

		String[] columnNames = setClause.split(AppConstant.COMMA_SPLIT);
		String insertQuery = AppConstant.INSERT + tableName + AppConstant.MASTER_TABLE_SUFFIX + AppConstant.OPEN_BRACKET
				+ setClause + AppConstant.VALUES
				+ String.join(AppConstant.COMMA, Collections.nCopies(columnNames.length, AppConstant.QUERY_PARAM))
				+ AppConstant.CLOSE_BRACKET;

		try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
			setParameters(preparedStatement, input, columnNames);

			int rowsAffected = preparedStatement.executeUpdate();
			if (rowsAffected > 0) {
				response = true;
			}

		} catch (Exception e) {
			Logging.error(AppConstant.ADD_MASTER_PROCESS_ERROR, e);
			return response;
		}
		return response;
	}

	/**
	 * Deletes data from the specified table based on the provided input, module,
	 * action, and table suffix.
	 *
	 * @param input
	 *            The input data for deletion.
	 * @param module
	 *            The module for which data is deleted.
	 * @param action
	 *            The action to be performed (approve,rectify or reject).
	 * @param tableSuffix
	 *            The suffix for the table from which data is deleted.
	 * @return A boolean indicating whether the data deletion was successful.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean deleteData(Map<String, Object> input, Module module, String action, String tableSuffix) {
		boolean response = false;
		String tableName = module.getEntityName();
		List<Field> fields = module.getFields();
		StringBuilder whereClause = new StringBuilder();
		List<Object> parameterValues = new ArrayList<>();
		List<Bean> beans = module.getBeans();

		Map<String, Object> dataMap = (Map<String, Object>) input.get(AppConstant.PARENT_DATA);
		if (AppConstant.MASTER_TABLE_SUFFIX.equals(tableSuffix)) {
			dataMap.put(AppConstant.APPROVE_BY, "superuser");
			dataMap.put(AppConstant.APPROVE_DATE_TIME, new Date());
		} else if (AppConstant.TEMP_TABLE_SUFFIX.equals(tableSuffix) && AppConstant.REJECT.equalsIgnoreCase(action)) {
			dataMap.put(AppConstant.REJECT_REMARK, input.get(AppConstant.REJECT_REMARK));
		}

		for (Field field : fields) {
			if (field.isPrimaryKey()) {
				String fieldName = field.getName();
				Object value = dataMap.entrySet().stream()
						.filter(entry -> entry.getKey().equalsIgnoreCase(fieldName.toLowerCase()))
						.map(Map.Entry::getValue).findFirst().orElse(null);

				if (value != null && whereClause.length() > 0) {
					whereClause.append(AppConstant.AND);
				}
				whereClause.append(fieldName).append(AppConstant.EQUAL_QUERY_PARAM);
				parameterValues.add(value);
			}
		}
		if (whereClause.length() == 0) {
			return response;
		}

		String tableNameWithSuffix = tableName + tableSuffix;
		String deleteQuery = String.format(AppConstant.DELETE_QUERY, tableNameWithSuffix, whereClause);

		try (Connection connection = dataSource.getConnection();
				PreparedStatement preparedStatement = connection.prepareStatement(deleteQuery)) {
			connection.setAutoCommit(false);
			for (int i = 0; i < parameterValues.size(); i++) {
				preparedStatement.setObject(i + 1, parameterValues.get(i));
			}
			int rowsDeleted = preparedStatement.executeUpdate();

			if (rowsDeleted > 0) {
				response = handleDeleteResponse(beans, input, module, action, tableSuffix, connection, dataMap);
			} else {
				connection.rollback();
			}

		} catch (Exception e) {
			Logging.error(AppConstant.DELETE_PROCESS_ERROR + tableNameWithSuffix, e);
			return response;
		}
		return response;
	}

	/**
	 * Handles the response for data deletion based on the provided beans, input,
	 * module, action, table suffix, connection, and data map.
	 *
	 * @param beans
	 *            The list of beans associated with the module.
	 * @param input
	 *            The input data for deletion.
	 * @param module
	 *            The module for which data is deleted.
	 * @param action
	 *            The action to be performed (approve or reject).
	 * @param tableSuffix
	 *            The suffix for the table from which data is deleted.
	 * @param connection
	 *            The database connection.
	 * @param dataMap
	 *            A map containing the data to be deleted.
	 * @return A boolean indicating whether the data deletion response was
	 *         successful.
	 */
	private boolean handleDeleteResponse(List<Bean> beans, Map<String, Object> input, Module module, String action,
			String tableSuffix, Connection connection, Map<String, Object> dataMap) {
		boolean response = false;
		boolean result = false;
		boolean histResult = false;
		try {
			if (beans != null) {
				for (Bean bean : beans) {
					result = handleDeleteForBean(bean, input, tableSuffix, connection);
				}
				if (result && AppConstant.TEMP_TABLE_SUFFIX.equals(tableSuffix)) {
					String request = dataMap.get(AppConstant.REQUEST).toString();
					histResult = addToHist(input, module, request, action, connection);
				}

			} else {
				String request = dataMap.get(AppConstant.REQUEST).toString();
				if (AppConstant.TEMP_TABLE_SUFFIX.equals(tableSuffix)) {
					histResult = addToHist(input, module, request, action, connection);
				}

			}

			if (histResult || AppConstant.MASTER_TABLE_SUFFIX.equals(tableSuffix)) {
				connection.commit();
				response = true;
			} else {
				connection.rollback();
			}

		} catch (Exception e) {
			Logging.error(AppConstant.DELETE_FAILED, e);
		}
		return response;

	}

	/**
	 * Handles the deletion response for a specified bean, input, table suffix, and
	 * connection.
	 *
	 * @param bean
	 *            The bean for which data is deleted.
	 * @param input
	 *            The input data for deletion.
	 * @param tableSuffix
	 *            The suffix for the table from which data is deleted.
	 * @param connection
	 *            The database connection.
	 * @return A boolean indicating whether the data deletion response was
	 *         successful.
	 */
	@SuppressWarnings("unchecked")
	private boolean handleDeleteForBean(Bean bean, Map<String, Object> input, String tableSuffix,
			Connection connection) {
		List<Map<String, Object>> subBeanDataMap = (List<Map<String, Object>>) input.get(AppConstant.BEAN_DATA);
		boolean result = false;

		if (subBeanDataMap != null) {
			for (Map<String, Object> beanMap : subBeanDataMap) {
				beanMap.put("REJECT_REMARK", input.get("REJECT_REMARK"));
				result = deleteDataForBean(beanMap, bean, tableSuffix, connection);
			}
		}

		return result;
	}

	/**
	 * Deletes data from the child master table in the database for a specified
	 * bean.
	 *
	 * @param input
	 *            The input data for deletion.
	 * @param bean
	 *            The bean for which data is deleted.
	 * @param tableSuffix
	 *            The suffix for the table from which data is deleted.
	 * @param connection
	 *            The database connection.
	 * @return A boolean indicating whether the data deletion was successful.
	 */
	@Override
	public boolean deleteDataForBean(Map<String, Object> input, Bean bean, String tableSuffix, Connection connection) {
		boolean response = false;
		String tableName = bean.getEntityName();
		List<Field> fields = bean.getFields();
		StringBuilder whereClause = new StringBuilder();
		List<Object> parameterValues = new ArrayList<>();
		if (AppConstant.MASTER_TABLE_SUFFIX.equals(tableSuffix)) {
			input.put(AppConstant.APPROVE_BY, "superuser");
			input.put(AppConstant.APPROVE_DATE_TIME, new Date());
		}
		for (Field field : fields) {
			if (field.isPrimaryKey()) {
				String fieldName = field.getName();
				Object value = input.entrySet().stream()
						.filter(entry -> entry.getKey().equalsIgnoreCase(fieldName.toLowerCase()))
						.map(Map.Entry::getValue).findFirst().orElse(null);

				if (value != null && whereClause.length() > 0) {
					whereClause.append(AppConstant.AND);
				}
				whereClause.append(fieldName).append(AppConstant.EQUAL_QUERY_PARAM);
				parameterValues.add(value);
			}
		}
		if (whereClause.length() == 0) {
			return response;
		}

		String tableNameWithSuffix = tableName + tableSuffix;
		String deleteQuery = String.format(AppConstant.DELETE_QUERY, tableNameWithSuffix, whereClause);

		try (PreparedStatement preparedStatement = connection.prepareStatement(deleteQuery)) {

			for (int i = 0; i < parameterValues.size(); i++) {
				preparedStatement.setObject(i + 1, parameterValues.get(i));
			}
			int rowsDeleted = preparedStatement.executeUpdate();

			if (rowsDeleted > 0) {
				response = true;
			}

		} catch (Exception e) {
			Logging.error(AppConstant.DELETE_PROCESS_ERROR + tableNameWithSuffix, e);
			return response;
		}

		return response;
	}

	/**
	 * Performs an update operation on the database for the specified data and
	 * module.
	 *
	 * @param data
	 *            The data to be updated in the database.
	 * @param module
	 *            The module for which the update operation is performed.
	 * @return A boolean indicating whether the update operation was successful.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean performUpdate(Map<String, Object> data, Module module, String tableSuffix) {
		boolean response = false;
		boolean primaryKeyAppended = false;
		List<Field> fields = module.getFields();
		List<Object> primaryKeyValues = new ArrayList<>();
		StringBuilder setClause = new StringBuilder();
		List<Object> setValues = new ArrayList<>();
		StringBuilder whereClause = new StringBuilder();
		String tableName = module.getEntityName();
		List<Bean> beans = module.getBeans();
		Map<String, Object> dataMap = (Map<String, Object>) data.get(AppConstant.PARENT_DATA);
		dataMap.put(AppConstant.APPROVE_BY, "superuser");
		dataMap.put(AppConstant.APPROVE_DATE_TIME, new Date());

		for (Field field : fields) {
			if (field.isPrimaryKey()) {
				whereClause.append(field.getName()).append(AppConstant.AND_CONDITION);
				primaryKeyValues.add(dataMap.get(field.getName()));
				primaryKeyAppended = true;
			} else {
				setClause.append(field.getName()).append(AppConstant.APPEND_PARAM);
				setValues.add(dataMap.get(field.getName()));
			}
		}
		setClause.append(AppConstant.UPDATED_BY).append(AppConstant.APPEND_PARAM);
		setValues.add(dataMap.get((AppConstant.UPDATED_BY)));

		setClause.append(AppConstant.UPDATED_DATE_TIME).append(AppConstant.APPEND_PARAM);
		setValues.add(dataMap.get((AppConstant.UPDATED_DATE_TIME)));

		setClause.append(AppConstant.APPROVE_BY).append(AppConstant.APPEND_PARAM);
		setValues.add(dataMap.get((AppConstant.APPROVE_BY)));

		setClause.append(AppConstant.APPROVE_DATE_TIME).append(AppConstant.APPEND_PARAM);
		setValues.add(dataMap.get((AppConstant.APPROVE_DATE_TIME)));

		if (AppConstant.TEMP_TABLE_SUFFIX.equalsIgnoreCase(tableSuffix)) {
			setClause.append(AppConstant.STATUS).append(AppConstant.APPEND_PARAM);
			setValues.add(AppConstant.RECTIFY);

			setClause.append(AppConstant.RECTIFY_REMARK).append(AppConstant.APPEND_PARAM);
			setValues.add(data.get(AppConstant.RECTIFY_REMARK));
		}
		setClause.setLength(setClause.length() - 1);

		if (primaryKeyAppended) {
			whereClause.setLength(whereClause.length() - AppConstant.AND.length());
		}
		int count = 0;

		String updateQuery = String.format(AppConstant.UPDATE_QUERY, tableName, tableSuffix, setClause, whereClause);

		try (Connection connection = dataSource.getConnection();
				PreparedStatement preparedStatement = connection.prepareStatement(updateQuery)) {
			connection.setAutoCommit(false);
			List<Object> allValues = new ArrayList<>();
			allValues.addAll(setValues);
			allValues.addAll(primaryKeyValues);

			for (Object value : allValues) {
				preparedStatement.setObject(count + 1, value);
				count++;
			}

			int rowsUpdated = preparedStatement.executeUpdate();
			response = handlePerformUpdateResponse(rowsUpdated, connection, beans, data, tableSuffix);

		} catch (Exception e) {
			Logging.error(AppConstant.UPDATE_PROCESS_ERROR, e);
			return response;

		}
		return response;
	}

	/**
	 * Handles the response for the update operation based on the rows updated,
	 * connection, beans, and data.
	 *
	 * @param rowsUpdated
	 *            The number of rows updated in the database.
	 * @param connection
	 *            The database connection.
	 * @param beans
	 *            The list of beans associated with the module.
	 * @param data
	 *            The data used in the update operation.
	 * @return A boolean indicating whether the update response handling was
	 *         successful.
	 */
	private boolean handlePerformUpdateResponse(int rowsUpdated, Connection connection, List<Bean> beans,
			Map<String, Object> data, String tableSuffix) {
		boolean response = false;
		try {
			if (rowsUpdated > 0 && beans != null) {
				response = handleUpdateWithBeans(connection, beans, data, tableSuffix);
			} else if (rowsUpdated > 0 && beans == null) {
				connection.commit();
				response = true;
			} else {
				connection.rollback();
				Logging.error(AppConstant.UPDATE_PROCESS_ERROR);
			}
		} catch (Exception e) {
			Logging.error(AppConstant.UPDATE_FAILED, e);
		}
		return response;
	}

	/**
	 * Handles the update response for a specified bean, input, table suffix, and
	 * connection.
	 *
	 * @param connection
	 *            The database connection.
	 * @param beans
	 *            The list of beans associated with the module.
	 * @param data
	 *            The data used in the update operation.
	 * @return A boolean indicating whether the update response handling was
	 *         successful.
	 */
	@SuppressWarnings("unchecked")
	private boolean handleUpdateWithBeans(Connection connection, List<Bean> beans, Map<String, Object> data,
			String tableSuffix) {
		boolean response = false;

		for (Bean bean : beans) {
			List<Map<String, Object>> subBeanDataMap = (List<Map<String, Object>>) data.get(AppConstant.BEAN_DATA);
			String rectifyRemark = (String) data.get(AppConstant.RECTIFY_REMARK);
			try {
				if (subBeanDataMap != null) {
					for (Map<String, Object> beanMap : subBeanDataMap) {

						beanMap.put("RECTIFY_REMARK", rectifyRemark);
						response = performUpdateForBean(beanMap, bean, connection, tableSuffix);
					}
					if (response) {
						connection.commit();
					} else {
						connection.rollback();
						Logging.error(AppConstant.UPDATE_PROCESS_ERROR);
					}
				}
			} catch (Exception e) {
				Logging.error(AppConstant.UPDATE_FAILED, e);
			}
		}

		return response;
	}

	/**
	 * Performs an update operation on the database for a specified bean and
	 * connection.
	 *
	 * @param data
	 *            The data to be updated in the database.
	 * @param bean
	 *            The bean for which the update operation is performed.
	 * @param connection
	 *            The database connection.
	 * @return A boolean indicating whether the update operation was successful.
	 */
	public boolean performUpdateForBean(Map<String, Object> data, Bean bean, Connection connection,
			String tableSuffix) {

		boolean response = false;
		boolean primaryKeyAppended = false;
		List<Field> fields = bean.getFields();
		List<Object> primaryKeyValues = new ArrayList<>();
		StringBuilder setClause = new StringBuilder();
		List<Object> setValues = new ArrayList<>();
		StringBuilder whereClause = new StringBuilder();
		String tableName = bean.getEntityName();
		data.put(AppConstant.APPROVE_BY, "superuser");
		data.put(AppConstant.APPROVE_DATE_TIME, new Date());
		for (Field field : fields) {
			if (field.isPrimaryKey()) {
				whereClause.append(field.getName()).append(AppConstant.AND_CONDITION);
				primaryKeyValues.add(data.get(field.getName()));
				primaryKeyAppended = true;
			} else {
				setClause.append(field.getName()).append(AppConstant.APPEND_PARAM);
				setValues.add(data.get(field.getName()));
			}
		}
		setClause.append(AppConstant.UPDATED_BY).append(AppConstant.APPEND_PARAM);
		setValues.add(data.get((AppConstant.UPDATED_BY)));
		setClause.append(AppConstant.UPDATED_DATE_TIME).append(AppConstant.APPEND_PARAM);
		setValues.add(data.get((AppConstant.UPDATED_DATE_TIME)));
		setClause.append(AppConstant.APPROVE_BY).append(AppConstant.APPEND_PARAM);
		setValues.add(data.get((AppConstant.APPROVE_BY)));
		setClause.append(AppConstant.APPROVE_DATE_TIME).append(AppConstant.APPEND_PARAM);
		setValues.add(data.get((AppConstant.APPROVE_DATE_TIME)));

		if (AppConstant.TEMP_TABLE_SUFFIX.equalsIgnoreCase(tableSuffix)) {
			setClause.append(AppConstant.STATUS).append(AppConstant.APPEND_PARAM);
			setValues.add(AppConstant.RECTIFY);

			setClause.append(AppConstant.RECTIFY_REMARK).append(AppConstant.APPEND_PARAM);
			setValues.add(data.get(AppConstant.RECTIFY_REMARK));

		}
		setClause.setLength(setClause.length() - 1);

		if (primaryKeyAppended) {
			whereClause.setLength(whereClause.length() - AppConstant.AND.length());
		}
		int count = 0;
		String updateQuery = String.format(AppConstant.UPDATE_QUERY, tableName, tableSuffix, setClause, whereClause);

		try (PreparedStatement preparedStatement = connection.prepareStatement(updateQuery)) {
			List<Object> allValues = new ArrayList<>();
			allValues.addAll(setValues);
			allValues.addAll(primaryKeyValues);

			for (Object value : allValues) {
				preparedStatement.setObject(count + 1, value);
				count++;
			}

			int rowsUpdated = preparedStatement.executeUpdate();

			if (rowsUpdated > 0) {
				response = true;
			}

		} catch (Exception e) {
			Logging.error(AppConstant.UPDATE_PROCESS_ERROR, e);
			return response;

		}
		return response;
	}

	/**
	 * Adds data to the historical table in the database for the specified input,
	 * module, request, action, and connection.
	 *
	 * @param input
	 *            The input data to be added to the historical table.
	 * @param module
	 *            The module for which data is added to the historical table.
	 * @param request
	 *            The request associated with the historical data addition.
	 * @param action
	 *            The action associated with the historical data addition.
	 * @param connection
	 *            The database connection.
	 * @return A boolean indicating whether the historical data addition was
	 *         successful.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean addToHist(Map<String, Object> input, Module module, String request, String action,
			Connection connection) {
		boolean response = false;
		String tableName = module.getEntityName();
		List<Bean> beans = module.getBeans();
		String tableNameWithSuffix = tableName + AppConstant.HIST_TABLE_SUFFIX;
		Map<String, Object> dataMap = (Map<String, Object>) input.get(AppConstant.PARENT_DATA);

		dataMap.entrySet().removeIf(entry -> Objects.isNull(entry.getValue()));

		StringBuilder setClauseBuilder = new StringBuilder();
		Set<String> keys = dataMap.keySet();
		for (String key : keys) {
			setClauseBuilder.append(key).append(AppConstant.COMMA_SPLIT);
		}

		if (setClauseBuilder.length() > 0) {
			setClauseBuilder.deleteCharAt(setClauseBuilder.length() - 1); // Remove the last comma
		}

		String setClause = setClauseBuilder.toString();
		String[] columnNames = setClause.split(AppConstant.COMMA_SPLIT);

		String insertQuery = AppConstant.INSERT + tableNameWithSuffix + AppConstant.OPEN_BRACKET + setClause
				+ AppConstant.VALUES
				+ String.join(AppConstant.COMMA, Collections.nCopies(columnNames.length, AppConstant.QUERY_PARAM))
				+ AppConstant.CLOSE_BRACKET;
		try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
			dataMap.put(AppConstant.STATUS, action);
			dataMap.put(AppConstant.REQUEST, request);

			setParameters(preparedStatement, dataMap, columnNames);

			int rowsAffected = preparedStatement.executeUpdate();

			if (rowsAffected > 0 && beans != null) {
				response = handleBeans(beans, input, request, action, connection);
			} else if (rowsAffected > 0 && beans == null) {
				response = true;

			}

		} catch (Exception e) {
			Logging.error(AppConstant.HIST_PROCESS_ERROR, e);
			return response;

		}
		return response;

	}

	/**
	 * Handles the response for the addition of historical data based on the rows
	 * affected, beans, input, request, action, and connection.
	 *
	 * @param beans
	 *            The list of beans associated with the module.
	 * @param input
	 *            The input data used in the addition of historical data.
	 * @param request
	 *            The request associated with the historical data addition.
	 * @param action
	 *            The action associated with the historical data addition.
	 * @param connection
	 *            The database connection.
	 * @return A boolean indicating whether the historical data addition response
	 *         handling was successful.
	 */
	@SuppressWarnings("unchecked")
	private boolean handleBeans(List<Bean> beans, Map<String, Object> input, String request, String action,
			Connection connection) {
		boolean response = false;

		if (beans != null) {
			for (Bean bean : beans) {
				List<Map<String, Object>> subBeanDataMap = (List<Map<String, Object>>) input.get(AppConstant.BEAN_DATA);
				if (subBeanDataMap != null) {
					for (Map<String, Object> beanMap : subBeanDataMap) {
						boolean result = addToHistBean(beanMap, bean, request, action, connection);
						if (result) {
							response = true;
						}
					}
				}
			}
		}

		return response;
	}

	/**
	 * Adds data to the historical table in the database for a specified bean,
	 * request, action, and connection.
	 *
	 * @param input
	 *            The input data to be added to the historical table.
	 * @param bean
	 *            The bean for which data is added to the historical table.
	 * @param request
	 *            The request associated with the historical data addition.
	 * @param action
	 *            The action associated with the historical data addition.
	 * @param connection
	 *            The database connection.
	 * @return A boolean indicating whether the historical data addition was
	 *         successful.
	 */
	@Override
	public boolean addToHistBean(Map<String, Object> input, Bean bean, String request, String action,
			Connection connection) {
		boolean response = false;
		String tableName = bean.getEntityName();

		String tableNameWithSuffix = tableName + AppConstant.HIST_TABLE_SUFFIX;

		input.entrySet().removeIf(entry -> Objects.isNull(entry.getValue()));

		StringBuilder setClauseBuilder = new StringBuilder();
		Set<String> keys = input.keySet();
		for (String key : keys) {
			setClauseBuilder.append(key).append(AppConstant.COMMA_SPLIT);
		}

		if (setClauseBuilder.length() > 0) {
			setClauseBuilder.deleteCharAt(setClauseBuilder.length() - 1); // Remove the last comma
		}

		String setClause = setClauseBuilder.toString();
		String[] columnNames = setClause.split(AppConstant.COMMA_SPLIT);

		String insertQuery = AppConstant.INSERT + tableNameWithSuffix + AppConstant.OPEN_BRACKET + setClause
				+ AppConstant.VALUES
				+ String.join(AppConstant.COMMA, Collections.nCopies(columnNames.length, AppConstant.QUERY_PARAM))
				+ AppConstant.CLOSE_BRACKET;
		try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
			input.put(AppConstant.STATUS, action);
			input.put(AppConstant.REQUEST, request);
			setParameters(preparedStatement, input, columnNames);

			int rowsAffected = preparedStatement.executeUpdate();
			if (rowsAffected > 0) {
				response = true;
			}

		} catch (Exception e) {
			Logging.error(AppConstant.HIST_PROCESS_ERROR, e);
			return response;

		}
		return response;

	}

	/**
	 * Retrieves data from the specified table in the database based on the provided
	 * table name, input, and sub-bean indicator.
	 *
	 * @param tableName
	 *            The name of the table from which data is retrieved.
	 * @param input
	 *            The input data for retrieval.
	 * @param isSubBean
	 *            A boolean indicating whether the retrieval is for a sub-bean.
	 * @return A list containing maps of the retrieved data.
	 */
	@Override
	public List<Map<String, Object>> getById(String tableName, Map<String, Object> input, boolean isSubBean) {
		List<Map<String, Object>> result = new ArrayList<>();

		try {
			String entity;
			if (tableName.contains(AppConstant.TEMP_TABLE_SUFFIX)) {
				entity = tableName.replaceAll(AppConstant.TEMP_TABLE1, AppConstant.EMPTY_STRING);
			} else {
				entity = tableName.replaceAll(AppConstant.MASTER_TABLE1, AppConstant.EMPTY_STRING);
			}
			List<String> primaryFields = isSubBean ? JsonToJavaConverter.getBeanPrimaryfields(entity)
					: JsonToJavaConverter.getPrimaryfields(entity);
			StringBuilder whereClause = new StringBuilder();
			List<Object> parameterValuesList = new ArrayList<>();
			for (String field : primaryFields) {
				if (input.containsKey(field)) {
					if (whereClause.length() > 0) {
						whereClause.append(AppConstant.AND);
					}
					whereClause.append(field).append(AppConstant.EQUAL_QUERY_PARAM);
					parameterValuesList.add(input.get(field));
				}
			}

			if (!parameterValuesList.isEmpty()) {
				String selectQuery = String.format(AppConstant.SELECT_QUERY1, tableName, whereClause);
				Object[] parameterValues = parameterValuesList.toArray();
				result = jdbcTemplate.queryForList(selectQuery, parameterValues);
			} else {
				Logging.error(AppConstant.NOT_PRIMARY_FIELD);
			}

		} catch (Exception e) {
			Logging.error(AppConstant.GET_PROCESS_ERROR + tableName, e);
		}

		return result;
	}

	/**
	 * Performs create, update, or delete (CUD) operations on the database for the
	 * specified input, entity, and request.
	 *
	 * @param input
	 *            The input data for CUD operations.
	 * @param module
	 *            The entity for which CUD operations are performed.
	 * @param request
	 *            The request associated with the CUD operations.
	 * @return A boolean indicating whether the CUD operations were successful.
	 */
	@Override
	public boolean doCUDOperations(Map<String, Object> input, Module module, String request) {
		boolean response = false;
		String tableName = module.getEntityName() + AppConstant.TEMP_TABLE_SUFFIX;
		List<Field> fields = module.getFields();
		StringBuilder setClauseBuilder = new StringBuilder();
		setClauseBuilder.append(fields.stream().map(Field::getName).collect(Collectors.joining(AppConstant.COMMA)));

		List<Map<String, Object>> masterResult = getById(module.getEntityName() + AppConstant.MASTER_TABLE_SUFFIX,
				input, false);

		if (AppConstant.ADD.equalsIgnoreCase(request)) {
			input.put(AppConstant.ADDED_BY, "superuser");
			input.put(AppConstant.ADDED_DATE_TIME, new Date());
			setClauseBuilder.append(AppConstant.COMMA).append(AppConstant.ADDED_BY).append(AppConstant.COMMA)
					.append(AppConstant.ADDED_DATE_TIME).append(AppConstant.COMMA).append(AppConstant.REQUEST)
					.append(AppConstant.COMMA).append(AppConstant.STATUS);
		} else if (AppConstant.UPDATE.equalsIgnoreCase(request)) {
			input.put(AppConstant.UPDATED_BY, "superuser");
			input.put(AppConstant.UPDATED_DATE_TIME, new Date());
			input.put(AppConstant.ADDED_BY, masterResult.get(0).get(AppConstant.ADDED_BY));
			input.put(AppConstant.ADDED_DATE_TIME, masterResult.get(0).get(AppConstant.ADDED_DATE_TIME));

			setClauseBuilder.append(AppConstant.COMMA).append(AppConstant.UPDATED_BY).append(AppConstant.COMMA)
					.append(AppConstant.UPDATED_DATE_TIME).append(AppConstant.COMMA).append(AppConstant.ADDED_BY)
					.append(AppConstant.COMMA).append(AppConstant.ADDED_DATE_TIME).append(AppConstant.COMMA)
					.append(AppConstant.REQUEST).append(AppConstant.COMMA).append(AppConstant.STATUS);
		} else {
			input.put(AppConstant.ADDED_BY, masterResult.get(0).get(AppConstant.ADDED_BY));
			input.put(AppConstant.ADDED_DATE_TIME, masterResult.get(0).get(AppConstant.ADDED_DATE_TIME));

			if (masterResult.get(0).get(AppConstant.UPDATED_DATE_TIME) != null) {
				input.put(AppConstant.UPDATED_BY, masterResult.get(0).get(AppConstant.UPDATED_BY));
				input.put(AppConstant.UPDATED_DATE_TIME, masterResult.get(0).get(AppConstant.UPDATED_DATE_TIME));
				setClauseBuilder.append(AppConstant.COMMA).append(AppConstant.ADDED_BY).append(AppConstant.COMMA)
						.append(AppConstant.ADDED_DATE_TIME).append(AppConstant.COMMA).append(AppConstant.UPDATED_BY)
						.append(AppConstant.COMMA).append(AppConstant.UPDATED_DATE_TIME).append(AppConstant.COMMA)
						.append(AppConstant.REQUEST).append(AppConstant.COMMA).append(AppConstant.STATUS);
			} else {
				setClauseBuilder.append(AppConstant.COMMA).append(AppConstant.ADDED_BY).append(AppConstant.COMMA)
						.append(AppConstant.ADDED_DATE_TIME).append(AppConstant.COMMA).append(AppConstant.REQUEST)
						.append(AppConstant.COMMA).append(AppConstant.STATUS);
			}
		}

		String setClause = setClauseBuilder.toString();
		String[] columnNames = setClause.split(AppConstant.COMMA_SPLIT);

		String insertQuery = generateInsertQuery(tableName, setClause, columnNames.length);
		if (AppConstant.ADD_ACTION.equalsIgnoreCase(request) || (AppConstant.UPDATE_ACTION.equalsIgnoreCase(request)
				|| AppConstant.DELETE_ACTION.equalsIgnoreCase(request))) {
			try (Connection connection = dataSource.getConnection();
					PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
				connection.setAutoCommit(false);
				input.put(AppConstant.STATUS, AppConstant.PENDING);
				input.put(AppConstant.REQUEST, request);
				setParameters(preparedStatement, input, columnNames);

				int result = preparedStatement.executeUpdate();

				if (result > 0) {
					connection.commit();
					response = true;
				} else {
					connection.rollback();
				}
			} catch (Exception e) {
				Logging.error(AppConstant.CUD_PROCESS_ERROR + request, e);
				return response;
			}
		}
		return response;

	}

	/**
	 * Performs create, update, or delete (CUD) operations on the database for the
	 * specified parent body, bean data, module, and request.
	 *
	 * @param parentBody
	 *            The input data for CUD operations.
	 * @param beanData
	 *            The data associated with the beans for CUD operations.
	 * @param module
	 *            The module for which CUD operations are performed.
	 * @param request
	 *            The request associated with the CUD operations.
	 * @return A boolean indicating whether the CUD operations were successful.
	 */
	@Override
	public boolean doCUDOperationWithBean(Map<String, Object> parentBody, List<Map<String, Object>> beanData,
			Module module, String request) {
		boolean response = false;
		String tableName = module.getEntityName() + AppConstant.TEMP_TABLE_SUFFIX;
		List<Field> fields = module.getFields();
		StringBuilder setClauseBuilder = new StringBuilder();
		setClauseBuilder.append(fields.stream().map(Field::getName).collect(Collectors.joining(AppConstant.COMMA)));

		List<Map<String, Object>> masterResult = getById(module.getEntityName() + AppConstant.MASTER_TABLE_SUFFIX,
				parentBody, false);

		if (AppConstant.ADD.equalsIgnoreCase(request)) {
			parentBody.put(AppConstant.ADDED_BY, "superuser");
			parentBody.put(AppConstant.ADDED_DATE_TIME, new Date());
			setClauseBuilder.append(AppConstant.COMMA).append(AppConstant.ADDED_BY).append(AppConstant.COMMA)
					.append(AppConstant.ADDED_DATE_TIME).append(AppConstant.COMMA).append(AppConstant.REQUEST)
					.append(AppConstant.COMMA).append(AppConstant.STATUS);
		} else if (AppConstant.UPDATE.equalsIgnoreCase(request)) {
			parentBody.put(AppConstant.UPDATED_BY, "superuser");
			parentBody.put(AppConstant.UPDATED_DATE_TIME, new Date());
			parentBody.put(AppConstant.ADDED_BY, masterResult.get(0).get(AppConstant.ADDED_BY));
			parentBody.put(AppConstant.ADDED_DATE_TIME, masterResult.get(0).get(AppConstant.ADDED_DATE_TIME));

			setClauseBuilder.append(AppConstant.COMMA).append(AppConstant.UPDATED_BY).append(AppConstant.COMMA)
					.append(AppConstant.UPDATED_DATE_TIME).append(AppConstant.COMMA).append(AppConstant.ADDED_BY)
					.append(AppConstant.COMMA).append(AppConstant.ADDED_DATE_TIME).append(AppConstant.COMMA)
					.append(AppConstant.REQUEST).append(AppConstant.COMMA).append(AppConstant.STATUS);
		} else {
			parentBody.put(AppConstant.ADDED_BY, masterResult.get(0).get(AppConstant.ADDED_BY));
			parentBody.put(AppConstant.ADDED_DATE_TIME, masterResult.get(0).get(AppConstant.ADDED_DATE_TIME));

			if (masterResult.get(0).get(AppConstant.UPDATED_DATE_TIME) != null) {
				parentBody.put(AppConstant.UPDATED_BY, masterResult.get(0).get(AppConstant.UPDATED_BY));
				parentBody.put(AppConstant.UPDATED_DATE_TIME, masterResult.get(0).get(AppConstant.UPDATED_DATE_TIME));
				setClauseBuilder.append(AppConstant.COMMA).append(AppConstant.ADDED_BY).append(AppConstant.COMMA)
						.append(AppConstant.ADDED_DATE_TIME).append(AppConstant.COMMA).append(AppConstant.UPDATED_BY)
						.append(AppConstant.COMMA).append(AppConstant.UPDATED_DATE_TIME).append(AppConstant.COMMA)
						.append(AppConstant.REQUEST).append(AppConstant.COMMA).append(AppConstant.STATUS);
			} else {
				setClauseBuilder.append(AppConstant.COMMA).append(AppConstant.ADDED_BY).append(AppConstant.COMMA)
						.append(AppConstant.ADDED_DATE_TIME).append(AppConstant.COMMA).append(AppConstant.REQUEST)
						.append(AppConstant.COMMA).append(AppConstant.STATUS);
			}
		}

		String setClause = setClauseBuilder.toString();
		String[] columnNames = setClause.split(AppConstant.COMMA_SPLIT);

		String insertQuery = generateInsertQuery(tableName, setClause, columnNames.length);
		if (AppConstant.ADD_ACTION.equalsIgnoreCase(request) || (AppConstant.UPDATE_ACTION.equalsIgnoreCase(request)
				|| AppConstant.DELETE_ACTION.equalsIgnoreCase(request))) {
			try {
				response = performCUDOperations(parentBody, beanData, module, request, insertQuery, columnNames);

			} catch (Exception e) {
				Logging.error(AppConstant.CUD_PROCESS_ERROR + request, e);
			}
		}
		return response;
	}

	/**
	 * Performs create, update, or delete (CUD) operations on the database for the
	 * specified parent body, bean data, module, request, fields, and insert query.
	 *
	 * @param parentBody
	 *            The input data for CUD operations.
	 * @param beanData
	 *            The data associated with the beans for CUD operations.
	 * @param module
	 *            The module for which CUD operations are performed.
	 * @param request
	 *            The request associated with the CUD operations.
	 * @param fields
	 *            The list of fields associated with the module.
	 * @param insertQuery
	 *            The SQL query for inserting data into the database.
	 * @return A boolean indicating whether the CUD operations were successful.
	 */
	private boolean performCUDOperations(Map<String, Object> parentBody, List<Map<String, Object>> beanData,
			Module module, String request, String insertQuery, String[] columnNames) {
		boolean response = false;

		parentBody.put(AppConstant.STATUS, AppConstant.PENDING);
		parentBody.put(AppConstant.REQUEST, request);
		try (Connection connection = dataSource.getConnection();
				PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {

			connection.setAutoCommit(false);
			setParameters(preparedStatement, parentBody, columnNames);
			int result = preparedStatement.executeUpdate();

			if (result > 0) {
				for (Bean bean : module.getBeans()) {
					if (!doCUDOperationForBean(connection, beanData, bean, request)) {
						response = false;
						break;
					}
					response = true;
				}
			}

			if (response) {
				connection.commit();
			} else {
				connection.rollback();
				Logging.error(AppConstant.CUD_PROCESS_ERROR + request);
			}

		} catch (Exception e) {
			Logging.error(AppConstant.CUD_PROCESS_ERROR, e);
		}

		return response;
	}

	/**
	 * Performs create, update, or delete (CUD) operations on the database for the
	 * specified connection, bean data, entity, and request.
	 *
	 * @param connection
	 *            The database connection.
	 * @param beanData
	 *            The data associated with the entity for CUD operations.
	 * @param bean
	 *            The bean for which CUD operations are performed.
	 * @param request
	 *            The request associated with the CUD operations.
	 * @return A boolean indicating whether the CUD operations were successful.
	 */
	@Override
	public boolean doCUDOperationForBean(Connection connection, List<Map<String, Object>> beanData, Bean bean,
			String request) {
		boolean flag = false;
		String tableName = bean.getEntityName() + AppConstant.TEMP_TABLE_SUFFIX;
		List<Field> fields = bean.getFields();

		for (Map<String, Object> beanBody : beanData) {
			StringBuilder setClauseBuilder = new StringBuilder(
					fields.stream().map(Field::getName).collect(Collectors.joining(AppConstant.COMMA)));
			List<Map<String, Object>> masterResult = getById(bean.getEntityName() + AppConstant.MASTER_TABLE_SUFFIX,
					beanBody, true);

			if (AppConstant.ADD.equalsIgnoreCase(request)) {
				beanBody.put(AppConstant.ADDED_BY, "superuser");
				beanBody.put(AppConstant.ADDED_DATE_TIME, new Date());
				setClauseBuilder.append(AppConstant.COMMA).append(AppConstant.ADDED_BY).append(AppConstant.COMMA)
						.append(AppConstant.ADDED_DATE_TIME).append(AppConstant.COMMA).append(AppConstant.REQUEST)
						.append(AppConstant.COMMA).append(AppConstant.STATUS);
			} else if (AppConstant.UPDATE.equalsIgnoreCase(request)) {
				beanBody.put(AppConstant.UPDATED_BY, "superuser");
				beanBody.put(AppConstant.UPDATED_DATE_TIME, new Date());
				beanBody.put(AppConstant.ADDED_BY, masterResult.get(0).get(AppConstant.ADDED_BY));
				beanBody.put(AppConstant.ADDED_DATE_TIME, masterResult.get(0).get(AppConstant.ADDED_DATE_TIME));

				setClauseBuilder.append(AppConstant.COMMA).append(AppConstant.UPDATED_BY).append(AppConstant.COMMA)
						.append(AppConstant.UPDATED_DATE_TIME).append(AppConstant.COMMA).append(AppConstant.ADDED_BY)
						.append(AppConstant.COMMA).append(AppConstant.ADDED_DATE_TIME).append(AppConstant.COMMA)
						.append(AppConstant.REQUEST).append(AppConstant.COMMA).append(AppConstant.STATUS);
			} else {
				beanBody.put(AppConstant.ADDED_BY, masterResult.get(0).get(AppConstant.ADDED_BY));
				beanBody.put(AppConstant.ADDED_DATE_TIME, masterResult.get(0).get(AppConstant.ADDED_DATE_TIME));

				if (masterResult.get(0).get(AppConstant.UPDATED_DATE_TIME) != null) {
					beanBody.put(AppConstant.UPDATED_BY, masterResult.get(0).get(AppConstant.UPDATED_BY));
					beanBody.put(AppConstant.UPDATED_DATE_TIME, masterResult.get(0).get(AppConstant.UPDATED_DATE_TIME));
					setClauseBuilder.append(AppConstant.COMMA).append(AppConstant.ADDED_BY).append(AppConstant.COMMA)
							.append(AppConstant.ADDED_DATE_TIME).append(AppConstant.COMMA)
							.append(AppConstant.UPDATED_BY).append(AppConstant.COMMA)
							.append(AppConstant.UPDATED_DATE_TIME).append(AppConstant.COMMA).append(AppConstant.REQUEST)
							.append(AppConstant.COMMA).append(AppConstant.STATUS);
				} else {
					setClauseBuilder.append(AppConstant.COMMA).append(AppConstant.ADDED_BY).append(AppConstant.COMMA)
							.append(AppConstant.ADDED_DATE_TIME).append(AppConstant.COMMA).append(AppConstant.REQUEST)
							.append(AppConstant.COMMA).append(AppConstant.STATUS);
				}
			}

			String setClause = setClauseBuilder.toString();
			String[] columnNames = setClause.split(AppConstant.COMMA_SPLIT);

			String insertQuery = generateInsertQuery(tableName, setClause, columnNames.length);

			try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {

				beanBody.put(AppConstant.REQUEST, request);
				beanBody.put(AppConstant.STATUS, AppConstant.PENDING);

				setParameters(preparedStatement, beanBody, columnNames);

				int result = preparedStatement.executeUpdate();

				if (result > 0) {
					flag = true;
				} else {
					flag = false;
					break; // If any operation fails, break the loop
				}

			} catch (Exception e) {
				Logging.error(AppConstant.CUD_PROCESS_ERROR + request, e);
			}
		}
		return flag;
	}

	/**
	 * Generates an SQL INSERT query for the specified table name, set clause, and
	 * number of parameters.
	 *
	 * @param tableName
	 *            The name of the table for which the INSERT query is generated.
	 * @param setClause
	 * 
	 *            The SET clause of the INSERT query.
	 * @param numParams
	 *            The number of parameters in the INSERT query.
	 * @return The generated SQL INSERT query.
	 */
	private String generateInsertQuery(String tableName, String setClause, int numParams) {
		return AppConstant.INSERT + tableName + AppConstant.OPEN_BRACKET + setClause + AppConstant.VALUES
				+ String.join(AppConstant.COMMA, Collections.nCopies(numParams, AppConstant.QUERY_PARAM))
				+ AppConstant.CLOSE_BRACKET;
	}

	/**
	 * Sets parameters in a prepared statement based on the provided fields and data
	 * map.
	 *
	 * @param preparedStatement
	 *            The prepared statement for which parameters are set.
	 * @param fields
	 *            The list of fields for which parameters are set.
	 * @param dataMap
	 *            The data map containing values for setting parameters.
	 * @return The index of the next parameter to be set.
	 */
	private void setParameters(PreparedStatement preparedStatement, Map<String, Object> dataMap, String[] columns) {
		try {
			for (int i = 0; i < columns.length; i++) {
				String fieldName = columns[i];
				Object value = dataMap.get(fieldName.trim());
				if (value instanceof String) {
					preparedStatement.setString(i + 1, (String) value);
				} else if (value instanceof Integer) {
					preparedStatement.setInt(i + 1, (Integer) value);
				} else if (value instanceof Boolean) {
					preparedStatement.setBoolean(i + 1, (Boolean) value);
				} else if (value instanceof Date) {
					preparedStatement.setTimestamp(i + 1, new Timestamp(((java.util.Date) value).getTime()));
				} else {
					ObjectMapper mapper = new ObjectMapper();
					String mapValue = mapper.writeValueAsString(value);
					preparedStatement.setString(i + 1, mapValue);
				}
			}
		} catch (Exception e) {
			Logging.error(AppConstant.PARAMETER_SET_ERROR, e);
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean doRectify(Module module, Map<String, Object> data, Map<String, Object> requestBody) {
		Map<String, Object> dataMap = (Map<String, Object>) data.get(AppConstant.PARENT_DATA);
		String request = dataMap.get(AppConstant.REQUEST).toString();
		String action = dataMap.get(AppConstant.STATUS).toString();
		try (Connection connection = dataSource.getConnection()) {
			connection.setAutoCommit(false);
			boolean result = addToHist(data, module, request, action, connection);
			if (result) {
				result = updateRectifiedData(module, requestBody, connection);
			}
			if (result) {
				connection.commit();
			}
		} catch (Exception e) {
			Logging.error("Error while rectifying data", e);
		}
		return true;
	}

	private int updateData(Connection connection, String tableName, List<Field> fields,
			Map<String, Object> requestBody) {
		int rowsUpdated = 0;
		boolean primaryKeyAppended = false;
		List<Object> primaryKeyValues = new ArrayList<>();
		StringBuilder setClause = new StringBuilder();
		List<Object> setValues = new ArrayList<>();
		StringBuilder whereClause = new StringBuilder();

		requestBody.put(AppConstant.STATUS, AppConstant.PENDING);
		requestBody.put(AppConstant.APPROVE_BY, "superuser");
		requestBody.put(AppConstant.APPROVE_DATE_TIME, new Date());

		for (Field field : fields) {
			if (field.isPrimaryKey()) {
				whereClause.append(field.getName()).append(AppConstant.AND_CONDITION);
				primaryKeyValues.add(requestBody.get(field.getName()));
				primaryKeyAppended = true;
			} else {
				setClause.append(field.getName()).append(AppConstant.APPEND_PARAM);
				setValues.add(requestBody.get(field.getName()));
			}
		}

		setClause.append(AppConstant.STATUS).append(AppConstant.APPEND_PARAM);
		setValues.add(requestBody.get(AppConstant.STATUS));

		setClause.append(AppConstant.UPDATED_BY).append(AppConstant.APPEND_PARAM);
		setValues.add(requestBody.get(AppConstant.UPDATED_BY));

		setClause.append(AppConstant.UPDATED_DATE_TIME).append(AppConstant.APPEND_PARAM);
		setValues.add(requestBody.get(AppConstant.UPDATED_DATE_TIME));

		setClause.append(AppConstant.APPROVE_BY).append(AppConstant.APPEND_PARAM);
		setValues.add(requestBody.get(AppConstant.APPROVE_BY));

		setClause.append(AppConstant.APPROVE_DATE_TIME).append(AppConstant.APPEND_PARAM);
		setValues.add(requestBody.get(AppConstant.APPROVE_DATE_TIME));

		setClause.append(AppConstant.RECTIFY_REMARK).append(AppConstant.APPEND_PARAM);
		setValues.add(AppConstant.EMPTY_STRING);

		setClause.setLength(setClause.length() - 1);
		if (primaryKeyAppended) {
			whereClause.setLength(whereClause.length() - AppConstant.AND.length());
		}
		int count = 0;

		String updateQuery = String.format(AppConstant.UPDATE_QUERY, tableName, AppConstant.TEMP_TABLE_SUFFIX,
				setClause, whereClause);

		try (PreparedStatement preparedStatement = connection.prepareStatement(updateQuery)) {
			List<Object> allValues = new ArrayList<>();
			allValues.addAll(setValues);
			allValues.addAll(primaryKeyValues);

			for (Object value : allValues) {
				preparedStatement.setObject(count + 1, value);
				count++;
			}

			rowsUpdated = preparedStatement.executeUpdate();

		} catch (Exception e) {
			Logging.error("Error occurred while updating data.", e);
		}

		return rowsUpdated;
	}

	@SuppressWarnings("unchecked")
	public boolean updateRectifiedData(Module module, Map<String, Object> requestBody, Connection connection) {
		List<Bean> beans = module.getBeans();
		boolean result = false;
		Map<String, Object> dataMap = (Map<String, Object>) requestBody.get(AppConstant.PARENT_DATA);
		int rowsUpdated = updateData(connection, module.getEntityName(), module.getFields(), dataMap);

		if (beans != null && rowsUpdated > 0) {
			result = updateBeansData(connection, beans, requestBody);
		} else if (rowsUpdated > 0) {
			result = true;
		}

		return result;
	}

	@SuppressWarnings("unchecked")
	private boolean updateBeansData(Connection connection, List<Bean> beans, Map<String, Object> requestBody) {
		boolean result = false;

		for (Bean bean : beans) {
			if (bean != null) {
				List<Map<String, Object>> beanMap = (List<Map<String, Object>>) requestBody.get(AppConstant.BEAN_DATA);
				for (Map<String, Object> beanData : beanMap) {
					int rowsUpdated = updateData(connection, bean.getEntityName(), bean.getFields(), beanData);
					if (rowsUpdated > 0) {
						result = true;
						break;
					}
				}
			}
		}

		return result;
	}

}