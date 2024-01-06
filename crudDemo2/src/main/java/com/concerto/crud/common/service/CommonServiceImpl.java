package com.concerto.crud.common.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.concerto.crud.common.bean.Bean;
import com.concerto.crud.common.bean.Entity;
import com.concerto.crud.common.bean.Module;
import com.concerto.crud.common.constant.AppConstant;
import com.concerto.crud.common.dao.CommonDAO;
import com.concerto.crud.common.util.JsonToJavaConverter;
import com.concerto.crud.common.util.Logging;
import com.concerto.crud.common.validationservice.ValidationService;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.service.CommonServiceImpl.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Service class providing methods for common operations on a
 *          specified module. Uses data validation, JSON conversion, and
 *          database interactions to perform create, update, read, delete, and
 *          search operations, as well as approval and rejection controls.
 *          Utilizes the CommonModuleDAO interface for database access.
 * @Version : 1.0
 */

@Service
public class CommonServiceImpl implements CommonService {

	@Autowired
	private CommonDAO commonDAO;

	@Autowired
	private ValidationService validationService;

	/**
	 * Retrieves data for a specified module based on the provided field name and
	 * value.
	 *
	 * @param fieldName
	 *            The name of the field used for filtering the data.
	 * @param value
	 *            The value to match in the specified field.
	 * @param moduleName
	 *            The name of the module for which data is requested.
	 * @return A map with the retrieved data or an error message if the operation
	 *         fails.
	 */
	@Override
	public List<Map<String, Object>> getData(String fieldName, Object value, String moduleName) {
		Module module = JsonToJavaConverter.moduleData(moduleName);
		List<Map<String, Object>> response = new ArrayList<>();
		List<String> primaryFields = JsonToJavaConverter.getPrimaryfields(moduleName);

		try {
			for (String field : primaryFields) {
				if (field.equals(fieldName)) {
					List<Map<String, Object>> dataRetrieve = commonDAO.executeGetData(fieldName, value, module);
					if (!dataRetrieve.isEmpty()) {
						return dataRetrieve;
					} else {
						Map<String, Object> message = new HashMap<>();
						message.put(AppConstant.COMMON_MODULE_MESSAGE, AppConstant.DATA_NOT_AVAILABLE);
						response.add(message);
						return response;
					}
				} else {
					Map<String, Object> message = new HashMap<>();
					message.put(AppConstant.COMMON_MODULE_MESSAGE, AppConstant.NOT_PRIMARY_KEY);
					response.add(message);
				}
			}
		} catch (Exception e) {
			Logging.error(AppConstant.DATA_READING_ERROR, e);
		}
		return response;
	}

	/**
	 * Retrieves all data for a specified module.
	 *
	 * @param moduleName
	 *            The name of the module for which all data is requested.
	 * @return A list of maps with all the retrieved data or an error message if the
	 *         operation fails.
	 */
	@Override
	public List<Map<String, Object>> getAllData(String moduleName) {
		Module module = JsonToJavaConverter.moduleData(moduleName);
		List<Map<String, Object>> dataRetrieve = commonDAO.executeGetAllData(module);
		if (dataRetrieve.isEmpty()) {
			Map<String, Object> message = new HashMap<>();
			message.put(AppConstant.COMMON_MODULE_MESSAGE, AppConstant.DATA_NOT_AVAILABLE_IN_MODULE + moduleName);
			dataRetrieve.add(message);
		}
		return dataRetrieve;
	}

	/**
	 * Performs create, update, or delete (CUD) operations for a specified module
	 * based on the provided request.
	 *
	 * @param requestBody
	 *            A map containing the data for the CUD operation.
	 * @param moduleName
	 *            The name of the module for which the CUD operation is performed.
	 * @param request
	 *            The type of CUD operation to be executed (create, update, or
	 *            delete).
	 * @return A map with the result of the CUD operation or an error message if the
	 *         operation fails.
	 */
	@Override
	public Map<String, Object> doCUDprocess(Map<String, Object> requestBody, String moduleName, String request) {
		Map<String, Object> result = new HashMap<>();
		String response = null;
		Module module = JsonToJavaConverter.moduleData(moduleName);
		Map<String, String> validationResult = validationService.checkValidation(module, requestBody);

		if (!validationResult.isEmpty()) {
			return new HashMap<>(validationResult);
		}

		List<Bean> beanList = module.getBeans();

		if (beanList != null) {
			response = handleCUDWithBeans(requestBody, module, request);
			if (AppConstant.SUCCESS.equals(response)) {
				result.put(AppConstant.COMMON_MODULE_MESSAGE, AppConstant.ADDED_FOR_APPROVAL + request);
			} else {
				result.put(AppConstant.COMMON_MODULE_MESSAGE, response);
			}
		} else {
			response = doCUDprocess(requestBody, module, request);
			result.put(AppConstant.COMMON_MODULE_MESSAGE, response);
		}

		return result;
	}

	/**
	 * Handles CUD operations for beans in the provided request body and the
	 * specified module.
	 *
	 * @param requestBody
	 *            A map containing the data for the CUD operation.
	 * @param module
	 *            The module for which the CUD operation is performed.
	 * @param request
	 *            The type of CUD operation to be executed (create, update, or
	 *            delete).
	 * @return A string indicating the result of the CUD operation for beans.
	 */
	private String handleCUDWithBeans(Map<String, Object> requestBody, Module module, String request) {
		Map<String, Object> parentBody = extractParentFields(requestBody);
		List<Map<String, Object>> beanData = extractBeanList(requestBody);

		String response = doCUDprocessForBean(parentBody, module, request);
		if (AppConstant.SUCCESS.equals(response)) {
			List<Bean> beanList = module.getBeans();
			for (int i = 0; i < beanList.size(); i++) {
				Bean bean = beanList.get(i);
				for (Map<String, Object> childBody : beanData) {
					response = doCUDprocessForBean(childBody, bean, request);
				}
				if (AppConstant.SUCCESS.equals(response)) {
					boolean result = commonDAO.doCUDOperationWithBean(parentBody, beanData, module, request);
					if (result) {
						response = AppConstant.SUCCESS;
					} else {
						response = AppConstant.DATA_INSERTION_FAILED;
					}
				}
			}
		}
		return response;

	}

	/**
	 * Handles CUD operations for a specific bean in the provided data and entity.
	 *
	 * @param requestBody
	 *            A map containing the data for the CUD operation.
	 * @param entity
	 *            The entity (bean) for which the CUD operation is performed.
	 * @param request
	 *            The type of CUD operation to be executed (create, update, or
	 *            delete).
	 * @return A string indicating the result of the CUD operation for the bean.
	 */
	private String doCUDprocessForBean(Map<String, Object> requestBody, Entity entity, String request) {
		String entityName = entity.getEntityName();
		boolean isSubBean = entity.isSubBean();
		String response = null;
		try {
			List<Map<String, Object>> tempResult = commonDAO.getById(entityName + AppConstant.TEMP_TABLE_SUFFIX,
					requestBody, isSubBean);
			List<Map<String, Object>> masterResult = commonDAO.getById(entityName + AppConstant.MASTER_TABLE_SUFFIX,
					requestBody, isSubBean);
			if (!tempResult.isEmpty()) {
				response = AppConstant.APPROVAL_PENDING;
			} else if (!masterResult.isEmpty() && AppConstant.ADD.equalsIgnoreCase(request)) {
				response = AppConstant.DATA_PRESENT;
			} else if (masterResult.isEmpty()
					&& (AppConstant.UPDATE.equalsIgnoreCase(request) || AppConstant.DELETE.equalsIgnoreCase(request))) {
				response = AppConstant.DATA_NOT_PRESENT;
			} else {
				response = AppConstant.SUCCESS;
			}
		} catch (Exception e) {
			Logging.error(AppConstant.DATA_INSERTION_FAILED, e);
		}
		return response;

	}

	/**
	 * Handles CUD operations for a specified entity (non-bean) in the provided data
	 * and entity.
	 *
	 * @param requestBody
	 *            A map containing the data for the CUD operation.
	 * @param module
	 *            The entity for which the CUD operation is performed.
	 * @param request
	 *            The type of CUD operation to be executed (create, update, or
	 *            delete).
	 * @return A string indicating the result of the CUD operation for the entity.
	 */
	private String doCUDprocess(Map<String, Object> requestBody, Module module, String request) {
		String entityName = module.getEntityName();
		boolean isSubBean = module.isSubBean();
		String response = null;
		List<Map<String, Object>> tempResult = commonDAO.getById(entityName + AppConstant.TEMP_TABLE_SUFFIX,
				requestBody, isSubBean);
		List<Map<String, Object>> masterResult = commonDAO.getById(entityName + AppConstant.MASTER_TABLE_SUFFIX,
				requestBody, isSubBean);
		if (!tempResult.isEmpty()) {
			response = AppConstant.APPROVAL_PENDING;
		} else if (!masterResult.isEmpty() && AppConstant.ADD.equalsIgnoreCase(request)) {
			response = AppConstant.DATA_PRESENT;
		} else if (masterResult.isEmpty()
				&& (AppConstant.UPDATE.equalsIgnoreCase(request) || AppConstant.DELETE.equalsIgnoreCase(request))) {
			response = AppConstant.DATA_NOT_PRESENT;
		} else {
			boolean result = commonDAO.doCUDOperations(requestBody, module, request);
			response = result ? AppConstant.ADDED_FOR_APPROVAL + request : AppConstant.DATA_INSERTION_FAILED;
		}
		return response;

	}

	/**
	 * Performs approval or rejection for a specified module based on the provided
	 * action.
	 *
	 * @param requestBody
	 *            A map containing the necessary input data for approval or
	 *            rejection.
	 * @param entityName
	 *            The name of the module for which approval or rejection is
	 *            performed.
	 * @param action
	 *            The action to be performed (approve or reject).
	 * @return A map with the result of the approval or rejection or an error
	 *         message if the operation fails.
	 */
	@Override
	public Map<String, Object> doApproveOrReject(Map<String, Object> requestBody, String entityName, String action) {
		Map<String, Object> response = new HashMap<>();
		Module module = JsonToJavaConverter.moduleData(entityName);
		boolean result = false;
		List<Bean> beans = module.getBeans();
		try {
			List<Map<String, Object>> dataList = commonDAO.getById(entityName + AppConstant.TEMP_TABLE_SUFFIX,
					requestBody, false);
			Map<String, Object> data = dataList.isEmpty() ? Collections.emptyMap() : dataList.get(0);

			Map<String, Object> combinedData = new HashMap<>();
			combinedData.put("REJECT_REMARK", requestBody.get("REJECT_REMARK"));
			combinedData.put("RECTIFY_REMARK", requestBody.get("RECTIFY_REMARK"));
			combinedData.put(AppConstant.PARENT_DATA, data);
			if (beans != null) {
				for (Bean bean : beans) {
					String beanName = bean.getEntityName();
					List<Map<String, Object>> subBeanData = commonDAO.getById(beanName + AppConstant.TEMP_TABLE_SUFFIX,
							requestBody, true);
					if (data.isEmpty() && subBeanData.isEmpty()) {
						response.put(AppConstant.COMMON_MODULE_MESSAGE, AppConstant.NO_REQUEST_PENDING);
						return response;
					} else {
						combinedData.put(AppConstant.BEAN_DATA, subBeanData);
						result = executeApproveOrRejectAction(module, action, combinedData);
					}
				}
			} else if (data.isEmpty()) {
				response.put(AppConstant.COMMON_MODULE_MESSAGE, AppConstant.NO_REQUEST_PENDING);
				return response;
			} else {
				result = executeApproveOrRejectAction(module, action, combinedData);
			}
			if (result) {
				response.put(AppConstant.COMMON_MODULE_MESSAGE, action + AppConstant.ACTION_SUCCESSFUL);
			} else {
				response.put(AppConstant.COMMON_MODULE_MESSAGE, action + AppConstant.ACTION_FAILED);
			}
		} catch (Exception e) {
			Logging.error(action + AppConstant.ACTION_FAILED, e);
		}

		return response;
	}

	/**
	 * Performs approval or rejection action for a specified module based on the
	 * provided data and action.
	 *
	 * @param module
	 *            The module for which approval or rejection is performed.
	 * @param action
	 *            The action to be performed (approve or reject).
	 * @param data
	 *            A map containing the data for the approval or rejection action.
	 * @return A boolean indicating whether the approval or rejection action was
	 *         successful.
	 */
	private boolean executeApproveOrRejectAction(Module module, String action, Map<String, Object> data) {
		boolean result = false;

		if (AppConstant.APPROVE.equalsIgnoreCase(action)) {
			return handleApproveAction(data, module, action);
		} else if (AppConstant.RECTIFY.equalsIgnoreCase(action)) {
			return commonDAO.performUpdate(data, module, AppConstant.TEMP_TABLE_SUFFIX);
		} else if (AppConstant.REJECT.equalsIgnoreCase(action)) {
			return commonDAO.deleteData(data, module, action, AppConstant.TEMP_TABLE_SUFFIX);
		}
		return result;
	}

	/**
	 * Handles the approval action for a specified data, module, and action.
	 *
	 * @param data
	 *            The data for which approval is performed.
	 * @param module
	 *            The module for which approval is performed.
	 * @param action
	 *            The action to be performed (approve).
	 * @return A boolean indicating whether the approval action was successful.
	 */
	@SuppressWarnings("unchecked")
	private boolean handleApproveAction(Map<String, Object> data, Module module, String action) {
		boolean tempDelete = false;
		try {
			Map<String, Object> dataMap = (Map<String, Object>) data.get(AppConstant.PARENT_DATA);
			String request = dataMap.get(AppConstant.REQUEST).toString();
			if (AppConstant.NEW_DATA_ACTION.equalsIgnoreCase(request)) {
				boolean addToMaster = commonDAO.addToMaster(data, module, action);
				if (addToMaster) {
					tempDelete = commonDAO.deleteData(data, module, action, AppConstant.TEMP_TABLE_SUFFIX);
				}
				return addToMaster && tempDelete;
			} else if (AppConstant.DELETE_ACTION.equalsIgnoreCase(request)) {
				boolean masterDelete = commonDAO.deleteData(data, module, action, AppConstant.MASTER_TABLE_SUFFIX);
				if (masterDelete) {
					tempDelete = commonDAO.deleteData(data, module, action, AppConstant.TEMP_TABLE_SUFFIX);
				}
				return masterDelete && tempDelete;
			} else if (AppConstant.UPDATE_ACTION.equalsIgnoreCase(request)) {
				boolean update = commonDAO.performUpdate(data, module, AppConstant.MASTER_TABLE_SUFFIX);
				if (update) {
					tempDelete = commonDAO.deleteData(data, module, action, AppConstant.TEMP_TABLE_SUFFIX);
				}
				return update && tempDelete;
			}
		} catch (Exception e) {
			Logging.error(action + AppConstant.ACTION_FAILED, e);
		}
		return false;
	}

	/**
	 * Extracts a list of beans from the given request body.
	 *
	 * @param requestBody
	 *            The request body containing the beans.
	 * @return A list of maps representing the bean data.
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> extractBeanList(Map<String, Object> requestBody) {
		List<Map<String, Object>> beanList = new ArrayList<>();

		for (Map.Entry<String, Object> entry : requestBody.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (AppConstant.BEANS.equals(key) && value instanceof List) {
				beanList.addAll((List<Map<String, Object>>) value);
			}
		}

		return beanList;
	}

	/**
	 * Extracts parent fields from the given request body, excluding beans.
	 *
	 * @param requestBody
	 *            The request body containing the data.
	 * @return A map containing the parent fields.
	 */
	public Map<String, Object> extractParentFields(Map<String, Object> requestBody) {
		Map<String, Object> parentFields = new HashMap<>();

		for (Map.Entry<String, Object> entry : requestBody.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (!AppConstant.BEANS.equals(key)) {
				parentFields.put(key, value);
			}
		}

		return parentFields;
	}

	@Override
	public Map<String, Object> doRectify(Map<String, Object> requestBody, String moduleName) {
		Map<String, Object> response = new HashMap<>();
		Module module = JsonToJavaConverter.moduleData(moduleName);
		boolean result = false;
		List<Bean> beans = module.getBeans();
		List<Map<String, Object>> dataList = commonDAO.getById(moduleName + AppConstant.TEMP_TABLE_SUFFIX, requestBody,
				false);
		Map<String, Object> data = dataList.isEmpty() ? Collections.emptyMap() : dataList.get(0);
		Map<String, Object> dataFromTemp = new HashMap<>();
		dataFromTemp.put(AppConstant.PARENT_DATA, data);
		if (beans != null) {
			for (Bean bean : beans) {
				response = processrectificationWithBean(bean, requestBody, dataFromTemp, module);
			}

		} else if (data != null) {
			response.put(AppConstant.COMMON_MODULE_MESSAGE, AppConstant.NO_REQUEST_PENDING);
			return response;
		} else {
			result = commonDAO.doRectify(module, dataFromTemp, requestBody);
			if (result) {
				response.put(AppConstant.COMMON_MODULE_MESSAGE, AppConstant.RECTIFICATION_SUCCESSFUL);
			}
		}
		return response;
	}

	private Map<String, Object> processrectificationWithBean(Bean bean, Map<String, Object> requestBody,
			Map<String, Object> data, Module module) {
		String beanName = bean.getEntityName();
		boolean result = false;
		Map<String, Object> response = new HashMap<>();
		List<Map<String, Object>> subBeanData = commonDAO.getById(beanName + AppConstant.TEMP_TABLE_SUFFIX, requestBody,
				true);
		if (data != null && subBeanData != null) {
			data.put(AppConstant.BEAN_DATA, subBeanData);
			Map<String, Object> parentBody = extractParentFields(requestBody);
			List<Map<String, Object>> beanData = extractBeanList(requestBody);
			Map<String, Object> input = new HashMap<>();
			input.put(AppConstant.PARENT_DATA, parentBody);
			input.put(AppConstant.BEAN_DATA, beanData);

			result = commonDAO.doRectify(module, data, input);
			if (result) {
				response.put(AppConstant.COMMON_MODULE_MESSAGE, AppConstant.RECTIFICATION_SUCCESSFUL);
			}
		} else {
			response.put(AppConstant.COMMON_MODULE_MESSAGE, AppConstant.NO_REQUEST_PENDING);
			return response;
		}
		return response;
	}
}
