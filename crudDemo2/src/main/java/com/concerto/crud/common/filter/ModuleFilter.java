package com.concerto.crud.common.filter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;

import com.concerto.crud.common.constant.AppConstant;
import com.concerto.crud.common.util.JsonToJavaConverter;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.filter.ModuleFilter.java
 * @Author : Gayatri Hande
 * @AddedDate : November 22, 2023 12:30:40 PM
 * @Purpose : This ModuleFilter class serves as a filter in the application to
 *          validate incoming requests based on the presence of a specified
 *          module. It ensures that only requests associated with valid modules
 *          are allowed to proceed through the filter chain, while requests for
 *          nonexistent modules trigger an error response with a 404 (Not Found)
 *          status.
 *
 * @Version : 1.0
 */

@Component
public class ModuleFilter implements Filter {

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		SpringBeanAutowiringSupport.processInjectionBasedOnServletContext(this, filterConfig.getServletContext());
	}

	public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
			throws IOException, ServletException {

		// Extracts the module name from the request URL
		String moduleName = extractModuleName(request);

		// Checks if the module is present
		if (JsonToJavaConverter.getModuleMap().containsKey(moduleName)) {
			// If the module is present, continues with the filter chain
			filterChain.doFilter(request, response);
		} else {
			// If the module is not present, generates an error response
			sendErrorResponse(response, AppConstant.MODULE_NAME_INCORRECT);
		}
	}

	// to extract the module name from the request URL
	private String extractModuleName(ServletRequest request) {
		String requestURI = ((HttpServletRequest) request).getRequestURI();
		return requestURI.substring(requestURI.lastIndexOf(AppConstant.HOME_URL) + 1);
	}

	private void sendErrorResponse(ServletResponse response, String errorMessage) throws IOException {
		// Sets the HTTP status code to 404 (Not Found)
		HttpServletResponse httpResponse = (HttpServletResponse) response;
		httpResponse.setStatus(HttpStatus.NOT_FOUND.value());

		// Sets the content type to JSON
		httpResponse.setContentType("application/json");

		// Write the error response to the response's PrintWriter
		response.getWriter().write("error :{" + errorMessage + "}");
		response.getWriter().flush();
	}

}
