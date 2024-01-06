package com.concerto.crud.common.validationservice;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Scope;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.stereotype.Service;

import com.concerto.crud.common.bean.Bean;
import com.concerto.crud.common.bean.Field;
import com.concerto.crud.common.bean.Module;
import com.concerto.crud.common.constant.AppConstant;
import com.concerto.crud.common.init.WebMVCConfiguration;
import com.concerto.crud.common.util.Logging;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.service.ValidationService.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Service class for performing validation checks on fields within a
 *          module. It supports checks for mandatory fields, character patterns,
 *          size constraints, and specific validation patterns.
 * @Version : 1.0
 */

@Service
@Scope("prototype")
public class ValidationService {

	private static final Pattern ALPHA_PATTERN = Pattern.compile("[a-z]", Pattern.CASE_INSENSITIVE);
	private static final Pattern NUMERIC_PATTERN = Pattern.compile("[0-9]", Pattern.CASE_INSENSITIVE);
	private static final Pattern SPECIAL_PATTERN = Pattern.compile("[@.\\[$&+,:;=?#|<>\\-_*()%!\\]^]",
			Pattern.CASE_INSENSITIVE);
	private static final Pattern SPACE_PATTERN = Pattern.compile("\\s", Pattern.CASE_INSENSITIVE);
	private static final Pattern AMOUNT_2D_PATTERN = Pattern.compile("^\\d+\\.\\d{2}$", Pattern.CASE_INSENSITIVE);
	private static final Pattern AMOUNT_1D_PATTERN = Pattern.compile("^\\d+\\.\\d{1}$", Pattern.CASE_INSENSITIVE);
	private static final Pattern COLORPICKER_PATTERN = Pattern.compile("^#[a-z0-9]+", Pattern.CASE_INSENSITIVE);
	private static final String IPV4_REGEX = "^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\."
			+ "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\." + "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\."
			+ "(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";
	private static final Pattern IPv4_PATTERN = Pattern.compile(IPV4_REGEX);
	private static MessageSource messageSource = WebMVCConfiguration.getMessageSource();

	String fieldName = "";
	String fieldType = "text";
	String checkValidation = "";
	int min = 0;
	int max = 0;
	String dateFormat = "";
	String message;
	Locale locale;

	public Map<String, String> isValid(Field field, Object obj, Map<String, String> result) {

		fieldName = field.getName();
		fieldType = field.getType();
		checkValidation = field.getValidation().getPattern();
		max = field.getValidation().getMaximum();
		min = field.getValidation().getMinimum();

		try {
			String inputText = null;
			if (obj != null) {
				inputText = String.valueOf(obj);
			}
			locale = LocaleContextHolder.getLocale();
			boolean validate = checkValidation != null && !checkValidation.trim().isEmpty();
			if ((validate && !checkMandatory(inputText, result)) || (inputText == null || inputText.isEmpty())) {

				return result;
			}
			if (validate
					&& (!checkMail(inputText, result) || !checkAmount(inputText, result)
							|| !checkColorpicker(inputText, result) || !checkIP(inputText, result)
							|| !checkPort(inputText, result) || !checkTel(inputText, result))
					|| !checkSize(inputText, result)) {
				return result;
			}
			if (dateFormat != null && !dateFormat.isEmpty() && !checkDate(inputText, result)) {
				message = messageSource.getMessage(AppConstant.VALIDATION_INVALID_DATE, null, locale)
						+ AppConstant.VALIDATION_SPACE + messageSource.getMessage(fieldName, null, locale);
				return result;
			}
			if (validate && !checkCharacters(inputText, result)) {
				return result;
			}

		} catch (Exception e) {
			message = messageSource.getMessage(AppConstant.VALIDATION_FAILED, null, locale);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	public Map<String, String> checkValidation(Module module, Map<String, Object> map) {
		Map<String, String> result = new HashMap<>();
		List<Map<String, String>> beansData = (List<Map<String, String>>) map.get("Beans");
		for (Field field : module.getFields()) {
			result = isValid(field, map.get(field.getName()), result);
		}

		if (beansData != null) {
			for (Bean bean : module.getBeans()) {
				for (Field field : bean.getFields()) {
					for (int i = 0; i < beansData.size(); i++) {
						Map<String, String> beanMap = beansData.get(i);
						result = isValid(field, beanMap.get(field.getName()), result);
					}
				}
			}
		}

		return result;
	}

	private boolean checkMandatory(String inputText, Map<String, String> result) {
		boolean valid = true;
		if (checkValidation.contains(AppConstant.VALIDATION_MANDATORY)
				&& (StringUtils.isBlank(inputText) || (AppConstant.FIELD_TYPE_DROP_DOWN.equals(fieldType)
						&& inputText.toLowerCase().contains(AppConstant.FIELD_DROP_DOWN_DEFAULTS)))) {
			valid = false;
			if (AppConstant.FIELD_TYPE_RADIO.equals(fieldType)) {
				message = messageSource.getMessage(AppConstant.VALIDATION_MANDATORY_SELECT, null, locale)
						+ AppConstant.VALIDATION_SPACE + messageSource.getMessage(fieldName, null, locale);
			} else if (AppConstant.FIELD_TYPE_TEXT.equals(fieldType)) {
				message = messageSource.getMessage(AppConstant.VALIDATION_MANDATORY_ENTER, null, locale)
						+ AppConstant.VALIDATION_SPACE + messageSource.getMessage(fieldName, null, locale);
			} else {
				message = messageSource.getMessage(AppConstant.VALIDATION_MANDATORY_SELECT, null, locale)
						+ AppConstant.VALIDATION_SPACE + messageSource.getMessage(fieldName, null, locale);
			}
			Logging.info(message);
			result.put(fieldName, message);
		}
		return valid;
	}

	private boolean checkCharacters(String inputText, Map<String, String> result) {
		boolean valid = true;
		boolean alpha = checkValidation.contains(AppConstant.VALIDATION_ALPHA);
		boolean numeric = checkValidation.contains(AppConstant.VALIDATION_NUMERIC);
		boolean special = checkValidation.contains(AppConstant.VALIDATION_SPECIAL);
		boolean space = checkValidation.contains(AppConstant.VALIDATION_SPACE);
		if (!alpha && !numeric && !special) {
			valid = true;
		} else {
			if (alpha) {
				Matcher matcher = ALPHA_PATTERN.matcher(inputText);
				inputText = matcher.replaceAll("");
			}
			if (numeric) {
				Matcher matcher = NUMERIC_PATTERN.matcher(inputText);
				inputText = matcher.replaceAll("");
			}
			if (special) {
				Matcher matcher = SPECIAL_PATTERN.matcher(inputText);
				inputText = matcher.replaceAll("");
			}
			if (space) {
				Matcher matcher = SPACE_PATTERN.matcher(inputText);
				inputText = matcher.replaceAll("");
			}
			if (inputText.equals("")) {
				valid = true;
			} else {
				setCharactersError(alpha, numeric, special, space, result);
				valid = false;
			}
		}
		return valid;
	}

	private void setCharactersError(boolean alpha, boolean numeric, boolean special, boolean space,
			Map<String, String> result) {
		String error = "";
		if (alpha && numeric) {
			error = messageSource.getMessage(AppConstant.VALIDATION_ENTER_ALPHANUMERIC, null, locale)
					+ AppConstant.VALIDATION_COMMA;
		} else if (alpha) {
			error = messageSource.getMessage(AppConstant.VALIDATION_ENTER_ALPHA, null, locale)
					+ AppConstant.VALIDATION_COMMA;
		} else if (numeric) {
			error = messageSource.getMessage(AppConstant.VALIDATION_ENTER_NUMERIC, null, locale)
					+ AppConstant.VALIDATION_COMMA;
		}

		if (special && space) {
			error += messageSource.getMessage(AppConstant.VALIDATION_ENTER_SPECIAL, null, locale)
					+ AppConstant.VALIDATION_SPACE
					+ messageSource.getMessage(AppConstant.VALIDATION_SIZE_AND, null, locale)
					+ AppConstant.VALIDATION_SPACE
					+ messageSource.getMessage(AppConstant.VALIDATION_ENTER_SPACE, null, locale);
		} else if (special) {
			if (error.endsWith(AppConstant.VALIDATION_COMMA)) {
				error = error.substring(0, error.length() - 2);
			}
			error += messageSource.getMessage(AppConstant.VALIDATION_SIZE_AND, null, locale)
					+ AppConstant.VALIDATION_SPACE
					+ messageSource.getMessage(AppConstant.VALIDATION_ENTER_SPECIAL, null, locale);
		} else if (space) {
			if (error.endsWith(AppConstant.VALIDATION_COMMA)) {
				error = error.substring(0, error.length() - 2);
			}
			error += messageSource.getMessage(AppConstant.VALIDATION_SIZE_AND, null, locale)
					+ AppConstant.VALIDATION_SPACE
					+ messageSource.getMessage(AppConstant.VALIDATION_ENTER_SPACE, null, locale);
		}
		if (error.endsWith(AppConstant.VALIDATION_COMMA)) {
			error = error.substring(0, error.length() - 2);
		}
		message = messageSource.getMessage(AppConstant.VALIDATION_ENTER_ONLY, null, locale)
				+ AppConstant.VALIDATION_SPACE + error + AppConstant.VALIDATION_SPACE
				+ messageSource.getMessage(AppConstant.VALIDATION_SIZE_CHARACTERS, null, locale);
		String e = fieldName + " : " + message;
		Logging.info(e);
		result.put(fieldName, message);
	}

	// this method checks for other than numeric
	public boolean checkTel(String tel, Map<String, String> result) {
		boolean valid = true;
		try {
			if (checkValidation.contains(AppConstant.VALIDATION_MOBILE)
					&& (!StringUtils.isNumeric(tel) || tel.length() != 10)) {
				valid = false;
			}
		} catch (Exception e) {
			valid = false;
		}
		if (!valid) {
			message = messageSource.getMessage(AppConstant.VALIDATION_INVALID_TELEPHONE, null, locale)
					+ AppConstant.VALIDATION_SPACE + messageSource.getMessage(fieldName, null, locale);
			Logging.info(message);
			result.put(fieldName, message);
		}
		return valid;
	}

	private boolean checkSize(String inputText, Map<String, String> result) {
		boolean valid = true;
		if (min > 0 && inputText.trim().length() < min) {
			valid = false;
		}
		if (valid && max > 0 && inputText.trim().length() > max) {
			valid = false;
		}
		String chars = messageSource.getMessage(AppConstant.VALIDATION_SIZE_CHARACTERS, null, locale);
		if (max == 1) {
			chars = messageSource.getMessage(AppConstant.VALIDATION_SIZE_CHARACTER, null, locale);
		}
		if (!valid && min > 0 && max > 0) {
			message = messageSource.getMessage(AppConstant.VALIDATION_SIZE_MINMAX, null, locale)
					+ AppConstant.VALIDATION_SPACE + min
					+ messageSource.getMessage(AppConstant.VALIDATION_SIZE_AND, null, locale) + max + chars;
			Logging.info(message);
			result.put(fieldName, message);
		} else if (!valid && max > 0) {
			message = messageSource.getMessage(AppConstant.VALIDATION_SIZE_MAX, null, locale)
					+ AppConstant.VALIDATION_SPACE + max + chars;
			Logging.info(message);
			result.put(fieldName, message);
		} else if (!valid && min > 0) {
			chars = messageSource.getMessage(AppConstant.VALIDATION_SIZE_CHARACTERS, null, locale);
			if (min == 1) {
				chars = messageSource.getMessage(AppConstant.VALIDATION_SIZE_CHARACTER, null, locale);
			}
			message = messageSource.getMessage(AppConstant.VALIDATION_SIZE_MIN, null, locale)
					+ AppConstant.VALIDATION_SPACE + min + chars;
			Logging.info(message);
			result.put(fieldName, message);
		}

		return valid;
	}

	private boolean checkMail(String inputText, Map<String, String> result) {
		boolean valid = true;
		if (checkValidation.contains(AppConstant.VALIDATION_EMAIL)) {
			valid = org.apache.commons.validator.routines.EmailValidator.getInstance().isValid(inputText);
		}
		if (!valid) {
			message = messageSource.getMessage(AppConstant.VALIDATION_INVALID_EMAIL, null, locale)
					+ AppConstant.VALIDATION_SPACE + messageSource.getMessage(fieldName, null, locale);
			Logging.info(message);
			result.put(fieldName, message);
		}
		return valid;
	}

	private boolean checkColorpicker(String inputText, Map<String, String> result) {
		boolean valid = true;
		if (checkValidation.contains(AppConstant.VALIDATION_COLORPICKER)) {
			if (inputText.length() != 7 || !inputText.startsWith("#")) {
				valid = false;
				String e = fieldName + ": Invalid ColorPicker length";
				Logging.info(e);
				result.put(fieldName, message);
			} else {
				Matcher matcher = COLORPICKER_PATTERN.matcher(inputText);
				inputText = matcher.replaceAll("");
				if (!inputText.equals("")) {
					valid = false;
				}
			}
		}
		if (!valid) {
			message = messageSource.getMessage(AppConstant.VALIDATION_INVALID_COLORPICKER, null, locale)
					+ AppConstant.VALIDATION_SPACE + messageSource.getMessage(fieldName, null, locale);
			Logging.info(message);
			result.put(fieldName, message);
		}
		return valid;
	}

	public boolean checkIP(String ip, Map<String, String> result) {
		boolean valid = true;
		if (checkValidation.contains(AppConstant.VALIDATION_IP)) {
			Matcher matcher = IPv4_PATTERN.matcher(ip);
			if (!matcher.matches()) {
				valid = false;
				message = messageSource.getMessage(AppConstant.VALIDATION_INVALID_IP, null, locale)
						+ AppConstant.VALIDATION_SPACE + messageSource.getMessage(fieldName, null, locale);
				Logging.info(message);
				result.put(fieldName, message);
			}
		}
		return valid;
	}

	public boolean checkPort(String port, Map<String, String> result) {
		boolean valid = true;
		try {
			if (checkValidation.contains(AppConstant.VALIDATION_PORT)) {
				int intPort = Integer.parseInt(port);
				if (intPort < 1 || intPort > 65535) {
					valid = false;
				}
			}
		} catch (Exception e) {
			valid = false;
		}
		if (!valid) {
			message = messageSource.getMessage(AppConstant.VALIDATION_INVALID_PORT, null, locale)
					+ AppConstant.VALIDATION_SPACE + messageSource.getMessage(fieldName, null, locale);
			Logging.info(message);
			result.put(fieldName, message);
		}
		return valid;
	}

	private boolean checkAmount(String inputText, Map<String, String> result) {
		boolean valid = true;
		if (checkValidation.contains(AppConstant.VALIDATION_AMOUNT)) {
			inputText = AMOUNT_2D_PATTERN.matcher(inputText).replaceAll("");
			inputText = AMOUNT_1D_PATTERN.matcher(inputText).replaceAll("");
			if (!inputText.equals("")) {
				valid = false;
				message = messageSource.getMessage(AppConstant.VALIDATION_INVALID_AMOUNT, null, locale)
						+ AppConstant.VALIDATION_SPACE + messageSource.getMessage(fieldName, null, locale);
				Logging.info(message);
				result.put(fieldName, message);
			}
		}
		return valid;
	}

	private boolean checkDate(String inputText, Map<String, String> result) {
		boolean valid = true;
		try {
			SimpleDateFormat format = new SimpleDateFormat(dateFormat);
			format.setLenient(true);
			format.parse(inputText);
		} catch (Exception e) {
			String e1 = fieldName + ": Invalid date format";
			Logging.info(e1);
			result.put(fieldName, ": Invalid date format");
			valid = false;
		}
		return valid;
	}

}
