package com.concerto.crud.common.service;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.service.CommonService.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose :Service interface defining common operations on a specified module.
 *          Includes methods for create, update, read, delete, search, approval,
 *          rejection, and reading all data. Intended to be implemented by
 *          classes providing module-specific interactions with the underlying
 *          data source through the CommonModuleDAO.
 * @Version : 1.0
 */

@Service
public interface CommonService {

	List<Map<String, Object>> getData(String fieldName, Object value, String moduleName);

	List<Map<String, Object>> getAllData(String moduleName);

	Map<String, Object> doCUDprocess(Map<String, Object> requestBody, String moduleName, String request);

	Map<String, Object> doApproveOrReject(Map<String, Object> requestBody, String moduleName, String action);

	Map<String, Object> doRectify(Map<String, Object> requestBody, String moduleName);
}
