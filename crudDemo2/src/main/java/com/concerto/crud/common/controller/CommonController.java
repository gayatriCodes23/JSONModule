package com.concerto.crud.common.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.concerto.crud.common.constant.AppConstant;
import com.concerto.crud.common.service.CommonService;
import com.concerto.crud.common.util.Logging;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.controller.CommonController.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Handles HTTP requests for creating, updating, reading, and
 *          deleting data for a specified module.
 * @Version : 1.0
 */

@RestController
@RequestMapping("/common_module")
public class CommonController {

	@Autowired
	private CommonService commonService;

	/**
	 * Handles HTTP GET requests to retrieve specific data for a given module,
	 * filtering based on a specified field name and value.
	 *
	 * @param fieldName
	 *            The name of the field used for filtering the data.
	 * @param value
	 *            The value to match in the specified field.
	 * @param moduleName
	 *            The name of the module for which data is requested.
	 * @return A ResponseEntity containing a map with the retrieved data or an error
	 *         message if the operation fails. The HTTP status indicates success
	 *         (OK) or failure (BAD_REQUEST).
	 */
	@GetMapping("/readData/{moduleName}")
	public ResponseEntity<List<Map<String, Object>>> getData(@RequestParam String fieldName, @RequestParam Object value,
			@PathVariable String moduleName) {
		List<Map<String, Object>> response = new ArrayList<>();
		try {
			response = commonService.getData(fieldName, value, moduleName);
		} catch (Exception e) {
			Logging.error(AppConstant.DATA_READING_ERROR, e);
			List<Map<String, Object>> errorDetails = new ArrayList<>();
			Map<String, Object> errorMap = new HashMap<>();
			errorMap.put(AppConstant.COMMON_MODULE_ERROR, AppConstant.DATA_READING_ERROR);
			errorDetails.add(errorMap);
			return new ResponseEntity<>(errorDetails, HttpStatus.BAD_REQUEST);
		}
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	/**
	 * Handles HTTP GET requests to retrieve all data for a specified module.
	 *
	 * @param moduleName
	 *            The name of the module for which all data is requested.
	 * @return A ResponseEntity containing a list of maps with all the retrieved
	 *         data or an error message if the operation fails. The HTTP status
	 *         indicates success (OK) or failure (BAD_REQUEST).
	 */
	@GetMapping("/readAllData/{moduleName}")
	public ResponseEntity<List<Map<String, Object>>> getAllData(@PathVariable String moduleName) {
		List<Map<String, Object>> response = new ArrayList<>();
		try {
			response = commonService.getAllData(moduleName);
		} catch (Exception e) {
			Logging.error(AppConstant.DATA_READING_ERROR, e);
			List<Map<String, Object>> errorDetails = new ArrayList<>();
			Map<String, Object> errorMap = new HashMap<>();
			errorMap.put(AppConstant.COMMON_MODULE_ERROR, AppConstant.DATA_READING_ERROR);
			errorDetails.add(errorMap);
			return new ResponseEntity<>(errorDetails, HttpStatus.BAD_REQUEST);
		}
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	/**
	 * Performs approval or rejection control for a specified module based on the
	 * provided action.
	 *
	 * @param input
	 *            A map containing the necessary input data for the approval or
	 *            rejection.
	 * @param moduleName
	 *            The name of the module for which the approval or rejection is
	 *            performed.
	 * @param action
	 *            The action to be performed (approve or reject).
	 * @return A ResponseEntity containing a map with the result of the approval or
	 *         rejection or an error message if the operation fails. The HTTP status
	 *         indicates success (OK) or failure (BAD_REQUEST).
	 */
	@PostMapping("{action}/{moduleName}")
	public ResponseEntity<Map<String, Object>> approveOrRejectControl(@RequestBody Map<String, Object> input,
			@PathVariable String moduleName, @PathVariable String action) {
		Map<String, Object> response = new HashMap<>();
		try {
			if (AppConstant.APPROVE.equalsIgnoreCase(action) || AppConstant.REJECT.equalsIgnoreCase(action)
					|| AppConstant.RECTIFY.equalsIgnoreCase(action)

			) {
				response = commonService.doApproveOrReject(input, moduleName, action);
			} else {
				response.put(AppConstant.COMMON_MODULE_ERROR, AppConstant.ACTION_INCORRECT);
			}
		} catch (Exception e) {
			Logging.error(AppConstant.ACTION_CONTROL_ERROR, e);
			response.put(AppConstant.COMMON_MODULE_ERROR, AppConstant.ACTION_CONTROL_ERROR);
			return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
		}
		return new ResponseEntity<>(response, HttpStatus.OK);

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
	 * @return A ResponseEntity containing a map with the result of the CUD
	 *         operation or an error message if the operation fails. The HTTP status
	 *         indicates success (OK) or failure (BAD_REQUEST).
	 */
	@PostMapping("process/{moduleName}")
	public ResponseEntity<Map<String, Object>> doCUDprocess(@RequestBody Map<String, Object> requestBody,
			@PathVariable String moduleName, @RequestParam String request) {
		Map<String, Object> response = new HashMap<>();
		try {
			response = commonService.doCUDprocess(requestBody, moduleName, request);
		} catch (Exception e) {
			Logging.error(AppConstant.CUD_PROCESS_ERROR + request, e);
			response.put(AppConstant.COMMON_MODULE_ERROR, AppConstant.CUD_PROCESS_ERROR + request);
			return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
		}
		return new ResponseEntity<>(response, HttpStatus.OK);
	}

	@PostMapping("rectifyAction/{moduleName}")
	public ResponseEntity<Map<String, Object>> doRectify(@RequestBody Map<String, Object> requestBody,
			@PathVariable String moduleName) {
		Map<String, Object> response = new HashMap<>();

		try {
			response = commonService.doRectify(requestBody, moduleName);
		} catch (Exception e) {
			Logging.error(AppConstant.DATA_RECTIFICATION_ERROR, e);
			response.put(AppConstant.COMMON_MODULE_ERROR, AppConstant.DATA_RECTIFICATION_ERROR);
			return new ResponseEntity<>(response, HttpStatus.BAD_REQUEST);
		}
		return new ResponseEntity<>(response, HttpStatus.OK);

	}

}
