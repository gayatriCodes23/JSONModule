package com.concerto.crud.common.constant;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.constant.AppConstant.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose :Provides a central location for defining constants used across the
 *          application, including SQL query formats, common error messages, and
 *          table suffixes,etc.
 * @Version : 1.0
 */

public class AppConstant {

	private AppConstant() {
		throw new IllegalStateException("AppConstant class");
	}

	public static final String EMPTY_STRING = "";
	public static final String GET_BY_PRIMARY_KEY_ERROR = "";
	public static final String COMMA = ", ";
	public static final String COMMA_SPLIT = ",";
	public static final String EQUAL_QUERY_PARAM = " = ?";
	public static final String QUERY_PARAM = "?";
	public static final String OPEN_BRACKET = " (";
	public static final String CLOSE_BRACKET = ")";
	public static final String APPEND_PARAM = "= ?,";
	public static final String MODULE_NAME_INCORRECT = "Module not found";
	public static final String COMMON_DATA_NOT_FOUND = "Data Not found for given details";
	public static final String COMMON_MODULE_MESSAGE = "message";
	public static final String COMMON_MODULE_ERROR = "error";

	public static final String CONNECTION = "connection";
	public static final String TABLE_NAME = "tableName";
	public static final String WHERECLAUSE = "whereClause";
	public static final String PARAMETER_VALUES = "parameterValues";
	public static final String INPUT = "input";
	public static final String MODULE = "module";
	public static final String FIELDS = "fields";
	public static final String SET_CLAUSE = "setClause";

	public static final String TEMP_TABLE_SUFFIX = "_TEMP";
	public static final String MASTER_TABLE_SUFFIX = "_MASTER";
	public static final String HIST_TABLE_SUFFIX = "_HIST";
	public static final String BEAN_NAME = "BeanName";
	public static final String BEAN_DATA = "BeanData";
	public static final String PARENT_DATA = "ParentData";
	public static final String RECTIFY_REMARK = "RECTIFY_REMARK";
	public static final String REJECT_REMARK = "REJECT_REMARK";
	public static final String BEANS = "Beans";

	public static final String TEMP_TABLE1 = "_TEMP$";
	public static final String MASTER_TABLE1 = "_MASTER$";
	public static final String INSERT = "INSERT INTO ";
	public static final String SELECT = " SELECT ";
	public static final String FROM = " FROM ";
	public static final String WHERE = " WHERE ";
	public static final String AND = " AND ";
	public static final String VALUES = ") VALUES (";
	public static final String AND_CONDITION = " = ? AND ";

	public static final String SELECT_QUERY = "SELECT %s FROM %s WHERE %s = ?";
	public static final String SELECT_ALL_QUERY = "SELECT * FROM %s";
	public static final String SELECT_QUERY1 = "SELECT * FROM %s WHERE %s";
	public static final String DELETE_QUERY = "DELETE FROM %s WHERE %s";
	public static final String UPDATE_QUERY = "UPDATE %s%s SET %s WHERE %s";

	public static final String INSERT_QUERY = "INSERT INTO %s (%s) VALUES %s";

	public static final String DATA_PRESENT = "Data already Present.";
	public static final String DATASOURCE_NULL = "Datasource cannot be null";
	public static final String DATA_NOT_PRESENT = "Data not present in Master table.";

	public static final String APP_PATH = System.getProperty("configPath");
	public static final String APP_CONFIG_PATH = APP_PATH + "//Configuration//";
	public static final String APP_CONFIG_FILE_PATH = APP_CONFIG_PATH + "config.properties";

	// Configuration Constants
	public static final String CONFIG_MAX_SESSIONS = "spring.user.max.session";
	public static final String CONFIG_DB_CLASS_NAME = "spring.datasource.driver-class-name";
	public static final String CONFIG_DB_URL = "spring.datasource.url";
	public static final String CONFIG_DB_USER_NAME = "spring.datasource.username";
	public static final String CONFIG_DB_PASS = "spring.datasource.password";
	public static final String CONFIG_DB_MIN_IDLE = "spring.datasource.min-idle";
	public static final String CONFIG_DB_MAX_ACTIVE = "spring.datasource.max-active";
	public static final String CONFIG_DB_DIALECT = "hibernate.dialect";
	public static final String CONFIG_DB_SHOW_SQL = "hibernate.show_sql";
	public static final String CONFIG_DB_FORMAT_SQL = "hibernate.format_sql";
	public static final String CONFIG_UPLOAD_MAX_SIZE = "spring.servlet.multipart.max-file-size";
	public static final String CONFIG_CROS_ALLOWED_ORIGINS = "spring.cros.allowed.origins";
	public static final String CONFIG_CROS_ALLOWED_HEADERS = "spring.cros.allowed.headers";
	public static final String CONFIG_CROS_ALLOWED_METHODS = "spring.cros.allowed.methods";
	public static final String CONFIG_CROS_ALLOW_CREDENTIALS = "spring.cros.allow.credentials";
	public static final String CONFIG_CROS_MAX_AGE = "spring.cros.max.age";

	public static final String LOGIN_PARAM_USER_ID = "j_username";
	public static final String LOGIN_PARAM_USER_PASS = "j_password";
	public static final String LOGIN_PARAM_OTP = "j_otp";
	public static final String LOGIN_PARAM_SHOW_PAGE = "showPage";
	public static final String LOGIN_FAILED_PAGE = "/loginError.htm";
	public static final String LOGIN_FAILED_PAGE_URL = LOGIN_FAILED_PAGE + "?error=true";
	public static final String LOGIN_PAGE_URL = "/loginUser.htm";
	public static final String LOGIN_URL = "/j_spring_security_check";
	public static final String FORGET_PASS_URL = "/validationForForgotPassword.htm";
	public static final String HOME_URL = "/";
	public static final String LOGOUT_SESSION_ID = "JSESSIONID";
	public static final String LOGOUT_URL = "/j_spring_security_logout";
	public static final String EXTENTION_HTM = "*.htm";
	public static final String EXTENTION_HTML = "*.html";
	public static final String EXTENTION_JSP = "*.jsp";
	public static final String PATTERN_ALPHA_NUMERIC_WITH_SPECIAL_CHARACTER = "alphaNumericWithSpecialCharacter";
	public static final String CONTEXT_PATH = "href=\"<%=request.getContextPath()+";
	public static final String MAKERCHECKER = "MakerChecker";
	public static final String INITIAL_ROLE = "InitialRole";
	public static final String USERTYPE = "userType";
	public static final String USER = "USER";
	public static final String DEFAULT = "CONCE";
	public static final String USER_ID = "USERID";
	public static final String SESSION_USER_ID = "USERID";
	public static final String SESSION_USER_NAME = "userName";
	public static final String SESSION_LAST_NAME = "lastName";
	public static final String SESSION_MENU = "menu";
	public static final String SESSION_LAST_LOGIN = "LastLogin";
	public static final String SESSION_USER_TYPE = "USERTYPE";
	public static final String SESSION_ROLE_INTERCODE = "ROLEINTERCODE";
	public static final String SESSION_ROLE_NAME = "ROLENAME";
	public static final String SESSION_USER_PRIORITY = "USERPRIORITY";
	public static final String SESSION_DEFAULT_LOGO = "DEFAULTLOGO";
	public static final String SESSION_FONT_COLOR = "FONTCOLOR";
	public static final String SESSION_FONT_NAME = "FONTNAME";
	public static final String SESSION_FONT_SIZE = "FONTSIZE";
	public static final String SESSION_MERCHANT_ID = "MERCHANT_ID";
	public static final String SESSION_UTILITY_BEAN = "UTILITY_BEAN";
	public static final String SESSION_FORM_BEAN = "FORM_BEAN";
	public static final String SESSION_APPROVE_FLAG_LIST = "ApproveFlagList";
	public static final String USER_PRIORITY = "0";
	public static final String USER_LOCKED = "LOCKED";
	public static final String USER_DELETE = "DELETE";
	public static final String USER_PASS_EXPIRE = "NO";
	public static final String MAINTENANCE_ACTION_ACTIVATE = "ACTIVATE";
	public static final String MAINTENANCE_ACTION_DEACTIVATE = "DEACTIVATE";
	public static final String MAINTENANCE_ACTION_RESET_PASS = "RESET PASSWORD";
	public static final String MAINTENANCE_ACTION_RESET_EXPIRY = "RESET EXPIRY";
	public static final String MAINTENANCE_ACTION_USER_CHANGE_PASS = "CHANGE PASSWORD";
	public static final String MAINTENANCE_ACTION_USER_FORGET_PASS = "FORGET PASSWORD";
	public static final String MAINTENANCE_ACTION_BLANK = "";
	public static final String USER_LOGIN_STATUS = "N";
	public static final int USER_LOGIN_ATTEMPTS = 0;
	public static final int USER_NOT_LOGIN_COUNT = 0;
	public static final int USER_LOGIN_COUNT = 1;
	public static final String USER_LOGIN_PAGE = "login";
	public static final String USER_LOGOUT_PAGE = "logout";
	public static final int SUPERUSER_TYPE = 1;
	public static final String SUPERUSER_PRIORITY = "1";
	public static final String SUPERUSER_ROLE = "1";
	public static final int SUPERUSER_LOGIN_ATTEMPTS = 0;
	public static final String USER_ACTIVE_STATUS = "ACTIVE";
	public static final String USER_DEACTIVE_STATUS = "DEACTIVE";
	public static final String PROFILE_ASSIGN_YES = "Y";
	public static final String PROFILE_ASSIGN_NO = "N";
	public static final String CHECK_USER_EXISTANCE = "checkUserExistance";
	public static final String ADD_SUPER_USER_TILE = "addSuperAdminUserTile";
	public static final String ADD_SUPER_USER = "addSuperUserPage";
	public static final String USER_HOME_PAGE = "redirect:/dashboard.htm";
	public static final String USER_LOGOUT_ACTION = "Logout";
	public static final String USER_LOGIN_ACTION = "Login";
	public static final String USER_PASS_PROFILE = "SIMPL";
	public static final String USER_LAYOUT_PROFILE = "CONCE";
	public static final String LAYOUT_DEFAULT_LOGO = "LOGO";
	public static final String DEFAULT_LOGO = "Concerto Software & Systems Pvt. Ltd.";
	public static final String USER_LOGIN_STATUS_FAILED = "F";
	public static final String USER_LOGIN_STATUS_SUCCESS = "S";
	public static final String INVALID_USER_CREDENTIALS = "common.invalid.credentials";
	public static final String LOGIN_AUTH_ERROR = "common.login.error";
	public static final String INVALID_OTP = "common.invalid.otp";
	public static final String EXPIRED_OTP = "common.expired.otp";
	public static final String LOGIN_USER_LOCKED = "common.user.locked";
	public static final String LOGIN_USER_DEACTIVATE = "common.user.deactive";
	public static final String LOGIN_MENU_FAILED = "common.menu.failed";
	public static final String LOGIN_OTP_SEND_FAILED = "common.otp.send.failed";
	public static final String LOGIN_ACCOUNT_EXPIRED = "common.account.expired";
	public static final String LOGIN_ERROR_MESSAGE = "message";
	public static final String LOGIN_EXCEPTION = "SPRING_SECURITY_LAST_EXCEPTION";
	public static final String USER_ACCOUNT_EXPIRED = "Account is expired.";
	public static final String LOGIN_SUCCESS = "Login successfully.";
	public static final String USER_LOG_ENTRY_FAILED = "User log entry failed.";
	public static final String LAST_LOGIN_FAILED = "Login failed - Something went wrong.";
	public static final String LAST_LOGIN_UPDATE_FAILED = "Last login update failed.";
	public static final String AUTO_RESET_PASS = "User reset password using forget password.";
	public static final String AUTO_CHANGE_PASS = "User change password.";
	public static final String USER_LOGOUT_SUCCESS = "Logout successfully";
	public static final String USER_PROFILES_NOT_FOUND = "User password/layout profile not found.";
	public static final String USER_PASS_RESET_FAILED = "User password reset failed.";
	public static final String USER_POST_APPROVE_FAILED = "User post approve detail set failed.";
	public static final String OTP_ENABLED = "otp.enabled";
	public static final String OTP_VALIDITY = "otp.validity";
	public static final String OTP_RETRY = "otp.retry";
	public static final int USER_PASS_EXPIRE_DAYS = 30;
	public static final int USER_EXPIRE_DAYS = 30;
	public static final String SESSION_SUBSCRIPTIONID = "subscriptionId";
	public static final String ALLOWED_USER_LIST = "allowedUserList";
	public static final String CUSTOMERID = "CUSTOMERID";
	public static final String DATA_INSERTION_ERROR = "An error occurred during data insertion";
	public static final String NO_DATA_INSERTED = "No data was inserted ";
	public static final String ERROR_OCCURRED = "Error occurred in ";
	public static final String DATA_UPDATION_ERROR = "Error while Updating the data ";
	public static final String DATA_DELETION_ERROR = "Error while Deleting the data ";
	public static final String MASTER_DATA_INSERTION_SUCCESS = "Data Inserted Successfully to master table";
	public static final String TEMP_DATA_INSERTION_SUCCESS = "Data inserted successfully to temp table for approval.";
	public static final String HIST_DATA_INSERTION_SUCCESS = "Data Inserted Successfully to hist table";
	public static final String INSERTION_DELETE_DATA_ERROR = "Error during data insertion for delete approval";
	public static final String DATA_INSERTION_FAILED = "Error occurred during data insertion";
	public static final String DATA_NOT_FOUND_IN_MASTER_TABLE = "Data not found in master table";
	public static final String DATABASE_CONNECTION_ERROR = "Error while setting up database connection";
	public static final String DATA_READING_ERROR = "Error occurred while reading data";
	public static final String UPDATE_PROCESS_ERROR = "An error occurred during the data update process.";
	public static final String HIST_PROCESS_ERROR = "An error occurred while adding data to hist table.";
	public static final String DELETE_PROCESS_ERROR = "An error occurred while deleting data from ";
	public static final String GET_PROCESS_ERROR = "An error occurred while fetching data from ";
	public static final String ADD_MASTER_PROCESS_ERROR = "An error occurred while adding data to master table.";
	public static final String FETCH_PROCESS_ERROR = "An error occurred fetching data from master table.";
	public static final String CUD_PROCESS_ERROR = "An error occurred in CUD process while attempting to ";
	public static final String PARAMETER_SET_ERROR = "Error occurred while setting parameters";
	public static final String DATA_RECTIFICATION_ERROR ="Error occurred while rectifying data";
	public static final String DATA_NOT_AVAILABLE = "No data Found for given value";
	public static final String NOT_PRIMARY_KEY = "Given value is not a primary key";
	public static final String JSON_READING_ERROR = "Error occurred during reading JSON file";
	public static final String APPROVE_CONTROL_ERROR = "Error occurred in approve control";
	public static final String REJECT_CONTROL_ERROR = "Error occurred in reject control";
	public static final String DATA_NOT_AVAILABLE_IN_MODULE = "No data Found in module";
	public static final String DATA_PROCESSING_ERROR = "Error occurred while processing data";
	public static final String ACTION_CONTROL_ERROR = "Error occurred while approving/rejecting the request";
	public static final String ACTION_INCORRECT = "Please check URL, incorrect action message provided";
	public static final String JSON_CONVERSION_ERROR = "An error occurred during JSON conversion";
	public static final String CHECK_EXISTING_DATA_ERROR = "Error occurred while checking Existing Data";
	public static final String NOT_PRIMARY_FIELD = "None of the input fields match any primary fields.";
	public static final String FETCH_TEMP_DATA_ERROR = "Error found while fetching temp data";
	public static final String APPROVAL_PENDING = "Data already present in temp table";
	public static final String DELETE_CRITERIA_NOT_PROVIDED = "No criteria provided for deletion.";
	public static final String SEARCH_ERROR = "Error Occurred in search operarion";
	public static final String NO_APPROVAL_REQUEST_PENDING = "No request found pending for approval of given data";
	public static final String NO_REJECTION_REQUEST_PENDING = "No request found pending for rejection of given data";
	public static final String NO_REQUEST_PENDING = "No request found pending for given data";
	public static final String REQUEST_APPROVED = "Request successfully approved.";
	public static final String REJECT_SUCCESSFUL = "Data rejected successfully";
	public static final String ACTION_SUCCESSFUL = " action successfully taken on request";
	public static final String ACTION_FAILED = " action failed";
	public static final String RECTIFY_NOT_ALLOWED = "Rectify action is not allowed for given request";
	public static final String UPDATE_FAILED = "Data updation failed";
	public static final String DELETE_FAILED = "Data Deletion failed";
	public static final String INVALID_REQUEST = "Invalid request type";
	public static final String UPDATE_SUCCESSFUL = "Update approved and data successfully updated in master table";
	public static final String RECTIFICATION_SUCCESSFUL = "Rectified data added successfully";
	public static final String DATA_NOT_FOUND_IN_TEMP_TABLE = "Data not available in temp Table";
	public static final String ADDED_FOR_APPROVAL = "Data successfully added to temp and pending for approval to ";
	public static final String LOGGING_ERROR_CLASS = "Error in class : ";
	public static final String LOGGING_ERROR_METHOD = "Error in method :";
	public static final String TXN_ID = "txn_id is : ";

	public static final String MODULE_NOT_FOUND = "Module not found with moduleName: {}";
	public static final String MODULE_RETRIEVING_ERROR = "An exception occurred while retrieving module data: ";
	public static final String MODULE_DATA_RETRIEVAL_FAILED = "Failed to retrieve module data";
	public static final String JSON_TO_JAVA_CONVERSION_FAILED = "Failed to read and convert JSON to Java";

	// Mail Configuration
	public static final String MAIL_FROM = "mail.From";
	public static final String MAIL_HOST = "mail.host";
	public static final String MAIL_PORT = "mail.port";
	public static final String MAIL_SMTP_AUTH = "mail.smtp.auth";
	public static final String MAIL_SMTP_AUTH_VALUE = "true";
	public static final String MAIL_SMTP_HOST = "mail.smtp.host";
	public static final String MAIL_SMTP_PORT = "mail.smtp.port";
	public static final String MAIL_BODY_CONTENT_TYPE = "text/html";
	public static final String MAIL_SUBJECT = "mail.subject";
	public static final String MAIL_SALUTATION = "mail.salutation";
	public static final String MAIL_SIGNATURE = "mail.signature";
	public static final String MAIL_USERNAME_PASS = "mail.username.password";
	public static final String MAIL_AUTH_USERNAME = "mail.auth.username";
	public static final String MAIL_AUTH_PASS = "mail.auth.password";
	public static final String MAIL_ACTIVE_USER = "mail.active.user";
	public static final String MAIL_DEACTIVE_USER = "mail.deactive.user";
	public static final String MAIL_USER_EXPIRE = "mail.user.expire";
	public static final String MAIL_USERID_EXPIRE = "mail.userid.expire";
	public static final String MAIL_USER_REPLACE = "$UserName";
	public static final String MAIL_USERID_REPLACE = "$UserId";
	public static final String MAIL_OTP_REPLACE = "$Otp";
	public static final String MAIL_OTP = "mail.otp";
	public static final String MAIL_PASS_REPLACE = "$Password";
	public static final String MAIL_USER_EXPIRE_REPLACE = "$UserIdExpire";
	public static final String MAIL_PASS_EXPIRE_REPLACE = "$PasswordExpire";
	public static final String CONFIG_LOAD_ERROR = "Configuration load failed, please check Configuration/config.properties file.";
	public static final String MAIL_SENT_MSG = "Sent message successfully.";

	// Menu Configuration
	public static final String MENU_ALLOW = "Y";
	public static final String MENU_FOLDER = "menu";
	public static final String MENU_PREFIX = "menu_";
	public static final String MENU_SUFFIX = ".jsp";
	public static final String DEFAULT_URL = "#";
	public static final String BLANK_URL = "";
	public static final String DEFAULT_ICON = "#";
	public static final String MENU_URL = "${url}";
	public static final String MENU_ID = "${id}";
	public static final String MENU_ICON = "${icon}";
	public static final String MENU_KEY = "${key}";
	public static final String MENU_CONTEXT_PATH = "${pageContext.servletContext.contextPath}";
	public static final String MENU_PAGE_START = "<%@taglib uri='http://www.springframework.org/tags' prefix='spring'%>\r\n<ul class=\"nav nav-pills nav-sidebar flex-column nav-child-indent nav-legacy nav-flat\"\r\n\tdata-widget=\"treeview\" role=\"menu\" data-accordion=\"false\">\r\n\t<li class=\"nav-item\" id=\"__dashboard\">\r\n\t\t<a href=\"/dashboard.htm\" class=\"nav-link\">\r\n\t\t\t<i class=\"nav-icon fas fa-tachometer-alt\"></i>\r\n\t\t\t<p><spring:message code=\"main.program.dashboard\" /></p>\r\n\t\t</a>\r\n\t</li>";
	public static final String MENU_PAGE_END = "\n</ul>";
	public static final String MENU_START = "\n\t<li class=\"nav-item has-treeview\" id=\"_${id}\">\r\n\t\t<a href=\"${url}\" class=\"nav-link\">\r\n\t\t\t<i class=\"nav-icon ${icon}\"></i>\r\n\t\t\t<p><spring:message code=\"${key}\" />${leftIcon}</p>\r\n\t\t</a>";
	public static final String MENU_END = "\n\t</li>";
	public static final String MENU_BODY_START = "\n\t\t<ul class='nav nav-treeview'>";
	public static final String MENU_BODY_END = "\n\t\t</ul>";
	public static final String SUB_MENU_START = "\n\t\t\t<li class=\"nav-item has-treeview\" id=\"_${id}\">\r\n\t\t\t\t<a href=\"${url}\" class=\"nav-link\">\r\n\t\t\t\t\t<i class=\"nav-icon ${icon}\"></i>\r\n\t\t\t\t\t<p><spring:message code=\"${key}\" />${leftIcon}</p>\r\n\t\t\t\t</a>";
	public static final String SUB_MENU_END = "\n\t\t\t</li>";
	public static final String SUB_MENU_BODY_START = "\n\t\t\t\t<ul class='nav nav-treeview'>";
	public static final String SUB_MENU_BODY_END = "\n\t\t\t\t</ul>";
	public static final String PERMISSION_BODY = "\n\t\t\t\t\t<li class='nav-item' id=\"_${id}\">\r\n\t\t\t\t\t\t<a href='${url}' class=\"nav-link\">\r\n\t\t\t\t\t\t\t<i class='nav-icon ${icon}'></i>\r\n\t\t\t\t\t\t\t<p><spring:message code=\"${key}\" /></p>\r\n\t\t\t\t\t\t</a>\r\n\t\t\t\t\t</li>";
	public static final String MENU_LEFT_ICON_HTML = "<i class=\"right fas fa-angle-left\"></i>";
	public static final String MENU_BLANK_HTML = "";
	public static final String MENU_LEFT_ICON = "${leftIcon}";
	public static final String MENU_FILE_GENERATE_ERROR = "Menu file generate eroor.";

	public static final String ALL = "ALL";
	public static final String MASTER_TABLE = "_MASTER";
	public static final String TEMP_TABLE = "_MASTER_TEMP";
	public static final String IS_ENABLE = "Y";
	public static final String PKG_CONTROLLER = ".controller";
	public static final String PACKAGE = "package ";
	public static final String NEW_LINE = "\n";
	public static final String PBL_CLASS = "public class ";
	public static final String PUBLIC = "public ";
	public static final String YES = "Y";
	public static final String NO = "N";
	public static final String PATTERN_MAX = "max=";
	public static final String PATTERN_MIN = "min=";
	public static final String PATTERN_NOT_BLANK = "NotBlank";
	public static final String PATTERN_NOT_EMPTY = "NotEmpty";
	public static final String PATTERN_NOT_NULL = "NotNull";
	public static final String PATTERN_ALPHA = "Alpha";
	public static final String PATTERN_ALPHA_NUMERIC = "AlphaNumeric";

	public static final String PATTERN_NUMERIC = "Numeric";
	public static final String ADD = "add";
	public static final String UPDATE = "update";
	public static final String DELETE = "delete";
	public static final String VIEW = "view";
	public static final String SEARCH = "search";
	public static final String DROP_DOWN_ALL = "Select";
	public static final String DROP_DOWN_NONE = "NONE";
	public static final String CONTROLLER_FOLDER = "controller";
	public static final String SERVICE_FOLDER = "service";
	public static final String DAO_FOLDER = "dao";
	public static final String BEAN_FOLDER = "bean";
	public static final String DEFAULT_TEMPLATE = "SIMPL";
	public static final String TILE = "Tile";
	public static final String SUCCESS = "success";
	public static final String SUCCESSFUL = "Successful";
	public static final String FAILED = "FAILED";
	public static final String TITLE = "title";
	public static final String BODY = "body";
	public static final String HIST_TABLE = "_MASTER_HIST";
	public static final String ERROR = "Error";

	public static final String FIELD_ID = "$fieldId";
	public static final String MODULE_NAME = "$moduleName";
	public static final String FIELD_LIST = "List";
	public static final String FIELD_CHANGE_DATA = "changeData";
	public static final String FIELD_CHANGE_TYPE = "changeType";
	public static final String FIELD_BLANK = "";
	public static final String FIELD_TYPE_TEXT = "text";
	public static final String FIELD_TYPE_RADIO = "radio";
	public static final String FIELD_TYPE_DROP_DOWN = "dropdown";
	public static final String FIELD_TYPE_DATE = "date";
	public static final String FIELD_TYPE_CHECK_BOX = "checkbox";
	public static final String FIELD_TYPE_EMAIL = "email";
	public static final String FIELD_INT_DATATYPE = "INT";
	public static final String FIELD_INTEGER_DATATYPE = "INTEGER";
	public static final String FIELD_LONG_DATATYPE = "LONG";
	public static final String FIELD_FLOAT_DATATYPE = "FLOAT";
	public static final String FIELD_DOUBLE_DATATYPE = "DOUBLE";
	public static final String FIELD_STRING_DATATYPE = "STRING";
	public static final String FIELD_DROP_DOWN_DEFAULTS = "select";
	public static final String FIELD_FILE_SEPERATOR = "-";
	public static final String FIELD_FILE_TEMP_FOLDER = "Temp";
	public static final String FIELD_FILE_TEMPORARY_FOLDER = "_temp";
	public static final String FIELD_FILE_TEMP_FIELD = "1";
	public static final String PAGE_TYPE_SINGLE = "Single Page";
	public static final String PAGE_TYPE_STANDARD = "Standard";
	public static final String PAGE_TYPE_SUB_BEAN = "Sub Bean";
	public static final String CHANGE_TYPE_ADD = "A";
	public static final String CHANGE_TYPE_UPDATE = "U";
	public static final String CHANGE_TYPE_DELETE = "D";
	public static final String JS_FOLDER_NAME = "formJS";
	public static final String POST = "POST";
	public static final String RESULT = "Result";
	public static final String GETPAGE_URL = "/getURLPage.htm";
	public static final String APPROVE = "approve";
	public static final String APPROVE_DETAIL = "approveDetail";
	public static final String REJECT = "reject";
	public static final String STATUS = "status";
	public static final String RECTIFY = "rectify";
	public static final String REQUEST_STATUS = "Status";
	public static final String ACTIVITY_TYPE = "activityType";
	public static final String APPROVE_BY = "APPROVE_BY";
	public static final String APPROVE_DATE_TIME = "APPROVE_DATE_TIME";
	public static final String ADDED_BY = "ADDED_BY";
	public static final String ADDED_DATE_TIME = "ADDED_DATE_TIME";
	public static final String UPDATED_BY = "UPDATED_BY";
	public static final String UPDATED_DATE_TIME = "UPDATED_DATE_TIME";
	// Search
	public static final String DAY_START_TIME = "00:00:00";
	public static final String DAY_END_TIME = "23:59:59";
	public static final String SEARCH_USER_ID = "searchUserId";
	public static final String SEARCH_USER_ID_LIST = "searchUserIdList";
	public static final String SEARCH_FROM_DATE = "searchFromDate";
	public static final String SEARCH_TO_DATE = "searchToDate";

	// init channel
	public static final String INT = "INT";// internet

	public static final String APPROVE_DETAIL_SCREEN_NAME = "Approve Detail";

	public static final String PATTERN_MAIL = "Mail";
	public static final String PATTERN_ALPHA_NUMERIC_SPACE = "AlphaNumericSpace";
	public static final String PATTERN_ALPHA_NUMERIC_SPECIAL = "AlphaNumWithSpec";
	public static final String PATTERN_AMOUNT = "Amount";
	public static final String MAX_LENGTH_SIZE = "100";

	public static final String JSP_FOLDER_NAME = "forms";

	// Audit Log
	public static final String AUDIT_SUCCESS = "Success";
	public static final String AUDIT_FAIL = "Fail";
	public static final String AUDIT_APPROVE_FLAG = "AAF";
	public static final String AUDIT_STATUS = "AS";
	public static final String AUDIT_ID = "AI";
	public static final String AUDIT_FIELD_VALUES = "AFV";
	public static final String AUDIT_USER_ID = "AUI";
	public static final String AUDIT_ACTION = "AA";
	public static final String AUDIT_OPTION = "AO";

	public static final String APPROVE_ROLE_CONFIG = "Approve Role Configuration";

	// Common Constants
	public static final String APPROVE_STATUS = "A";
	public static final String REJECT_STATUS = "R";
	public static final String PENDING_STATUS = "P";
	public static final String REMOVE_STATUS = "D";
	public static final String APPROVE_ERROR_STATUS = "E";
	public static final String APPROVE_STATUS_MESSAGE = "common.status.approve";
	public static final String REJECT_STATUS_MESSAGE = "common.status.reject";
	public static final String PENDING_STATUS_MESSAGE = "common.status.pending";
	public static final String REMOVE_STATUS_MESSAGE = "common.status.remove";
	public static final String APPROVE_ERROR_STATUS_MESSAGE = "common.status.error";
	public static final String INITIAL_APPROVE_REMARK = "Initial Configuration";
	public static final String AUTO_APPROVE_REMARK = "Auto Approved";
	public static final String FORM_DEPENDENCY_BEANS = "formDepenencyBeans";
	public static final String APPROVE_FLAG_LIST = "ApproveFlagList";

	public static final String POSTFIX_TILES = "Tile";
	public static final String POSTFIX_FORM = "Form";
	public static final String POSTFIX_RESULT = "Result";
	public static final String POSTFIX_RESULT_TILE = "ResultTile";
	public static final String POSTFIX_PENDING_LIST = "PendingList";
	public static final String POSTFIX_MASTER_BEAN = "MasterBean";
	public static final String POSTFIX_TEMP_BEAN = "TempBean";
	public static final String POSTFIX_HIST_BEAN = "HistBean";
	public static final String POSTFIX_FORM_BEAN = "FormBean";
	public static final String POSTFIX_LIST = "List";
	public static final String POSTFIX_FAILED_RESULT = "FailResult";
	public static final String POSTFIX_PAGE = "Page";

	public static final String PREFIX_SEARCH = "search";
	public static final String PREFIX_SEARCH_TILE = "Search";
	public static final String PREFIX_SCREEN = "Screen";
	public static final String PREFIX_GET = "get";
	public static final String PREFIX_GET_SEARCH = "getSearch";
	public static final String PREFIX_APPROVE = "approve";
	public static final String PREFIX_APPROVE_FLAG = "Approve ";
	public static final String PREFIX_REJECT = "reject";

	public static final String SERVERERROR = "serverError";
	public static final String UPDATEDDATETIMECOLUMN = "UPDATED_DATE_TIME";
	public static final String UPDATEDBYCOLUMN = "UPDATED_BY";
	public static final String APPROVE_DATETIME_COLUMN = "UPDATED_DATE_TIME";
	public static final String APPROVE_BY_COLUMN = "UPDATED_BY";
	public static final String SUCCESS_RECORD = "Success";
	public static final String FAILED_RECORD = "Failed";
	public static final String TOTAL_RECORD = "TotalRecord";
	public static final String APPROVE_FLAG_NO = "0";
	public static final String APPROVE_FLAG_YES = "1";
	public static final String APPROVE_PROGRAM = "main.program.approve";
	public static final String APPROVE_SUBMENU = "menu.submenu.";
	public static final String APPROVE_START = "Approve ";
	public static final String APPROVE_BLANK = "";
	public static final String GET_APPROVE_URL = "/getApprove";
	public static final String PENDING_APPROVALS = "PendingApprovals";
	public static final String TOTAL_PENDING_APPROVALS = "TotalPendingApprovals";
	public static final String PENDING_APPROVALS_TIME = "PendingApprovalsTime";
	public static final String GET_APPROVE_PAGE_URL = "Page.htm";
	public static final String PRIMARY_KEY_SPLITTER = "$$";

	public static final String UPLOAD_ACTION = "Upload";
	public static final String DOWNLOAD_ACTION = "Download";

	public static final String APPROVE_REQUESTED = "requested";
	public static final String APPROVE_FAILED = "failed";
	public static final String APPROVE_SUCCEED = "succeed";
	public static final String APPROVE_PENDING = "totalPendingRecs";
	public static final String APPROVE_KEY_SPLITTER = ",";
	public static final String APPROVE_REMARK_SPLITTER = ",";

	public static final String ADD_ACTION = "Add";
	public static final String GET_BY_ID_ACTION = "GetById";
	public static final String MANAGE_ACTION = "Manage";
	public static final String NEW_DATA_ACTION = "Add";
	public static final String UPDATE_ACTION = "Update";
	public static final String DELETE_ACTION = "Delete";
	public static final String VIEW_ACTION = "View";
	public static final String DEPENDENCY_ACTION = "Dependency";
	public static final String HISTORY_ACTION = "Hist";
	public static final String APPROVE_ACTION = "Approve";
	public static final String REJECT_ACTION = "Reject";
	public static final String REMOVE_ACTION = "Remove";
	public static final String REJECTED_ACTION = "Rejected";
	public static final String UPDATE_REJECTED_ACTION = "Update Rejected";
	public static final String SINGLE_APPROVE_ACTION = "Single Approve";
	public static final String SINGLE_REMOVE_ACTION = "Single Remove";
	public static final String SINGLE_REJECT_ACTION = " Single Reject";
	public static final String SEARCH_ACTION = "Search";
	public static final String MAINTENANCE_ACTION = "Maintenance";
	public static final String RESET_CHANGE_ACTION = "Reset/Change";
	public static final String CHANGE_PASS_ACTION = "Change Password";
	public static final String RESET_PASS_ACTION = "Reset Password";
	public static final String FORGET_PASS_ACTION = "Forgot Password";
	public static final String SESSION_EXPIRED_ACTION = "Session Expired";
	public static final String JSON_RESPONSE = "Response";
	public static final String LOGOUT_ACTION = "Logout";
	public static final String LOIN_ACTION = "Login";
	public static final String MAKER_CHECKER_PAGE = " Maker Checker Configuration";
	public static final String SUPER_USER_PAGE = " Super Admin User";
	public static final String URL_PREFIX_COMMON = "/{moduleName}/{action}/";
	public static final String URL_SUFFIX_PAGE = "page.htm";
	public static final String URL_SUFFIX_PROCESS = "process.htm";
	public static final String URL_SUFFIX_RECORD = "record.htm";
	public static final String URL_SUFFIX_SEARCH_PAGE = "searchpage.htm";
	public static final String URL_SUFFIX_SEARCH = "search.htm";
	public static final String URL_SUFFIX_CHECKER = "checker.htm";
	public static final String URL_SUFFIX_SINGLECHECKER = "singlechecker.htm";
	public static final String ORDER_BY_COLUMN = "updatedDateTime desc";
	public static final String COMMON_FORM_NAME = "formBean";
	public static final String SEARCH_FORM_BEAN = "search";
	public static final String PENDING_LIST = "PendingList";
	public static final String VIEW_PENDING_LIST = "ViewPendingList";
	public static final String APPROVE_LIST = "ApproveList";
	public static final String HISTORY_ID = "histId";
	public static final String PARENT_HISTORY_ID = "parentHistId";

	// Validation Messages
	public static final String VALIDATION_SPACE = "_";
	public static final String VALIDATION_COMMA = ", ";
	public static final String VALIDATION_MANDATORY_ENTER = "common.validation.mandatory.enter";
	public static final String VALIDATION_MANDATORY_SELECT = "common.validation.mandatory.select";
	public static final String VALIDATION_INVALID_EXTENSION = "common.validation.invalid.extension";
	public static final String VALIDATION_SIZE_MIN = "common.validation.size.min";
	public static final String VALIDATION_SIZE_MAX = "common.validation.size.max";
	public static final String VALIDATION_SIZE_MINMAX = "common.validation.size.minmax";
	public static final String VALIDATION_SIZE_CHARACTERS = "common.validation.size.characters";
	public static final String VALIDATION_SIZE_CHARACTER = "common.validation.size.character";
	public static final String VALIDATION_SIZE_AND = "common.validation.size.and";
	public static final String VALIDATION_INVALID_EMAIL = "common.validation.invalid.email";
	public static final String VALIDATION_INVALID_COLORPICKER = "common.validation.invalid.colorpicker";
	public static final String VALIDATION_INVALID_AMOUNT = "common.validation.invalid.amount";
	public static final String VALIDATION_INVALID_IP = "common.validation.invalid.ip";
	public static final String VALIDATION_INVALID_PORT = "common.validation.invalid.port";
	public static final String VALIDATION_INVALID_TELEPHONE = "common.validation.invalid.tel";
	public static final String VALIDATION_INVALID_DATE = "common.validation.invalid.date";
	public static final String VALIDATION_ENTER_ONLY = "common.validation.enter.only";
	public static final String VALIDATION_ENTER_ALPHA = "common.validation.enter.alpha";
	public static final String VALIDATION_ENTER_ALPHANUMERIC = "common.validation.enter.alphanumeric";
	public static final String VALIDATION_ENTER_NUMERIC = "common.validation.enter.numeric";
	public static final String VALIDATION_ENTER_SPECIAL = "common.validation.enter.special";
	public static final String VALIDATION_ENTER_SPACE = "common.validation.enter.space";
	public static final String VALIDATION_FAILED = "common.validation.failed";
	public static final String VALIDATION_ADD_ATLEAST_ONE = "common.add.atleast.one";
	public static final String VALIDATION_DEFAULT = "";
	public static final String VALIDATION_MANDATORY = "M";
	public static final String VALIDATION_EMAIL = "E";
	public static final String VALIDATION_COLORPICKER = "C";
	public static final String VALIDATION_IP = "I";
	public static final String VALIDATION_PORT = "P";
	public static final String VALIDATION_MOBILE = "T";
	public static final String VALIDATION_AMOUNT = "$";
	public static final String VALIDATION_ALPHA = "A";
	public static final String VALIDATION_NUMERIC = "N";
	public static final String VALIDATION_SPECIAL = "S";
	public static final String VALIDATION_SPECE = "_";
	public static final String VALIDATION_EQUAL_CONDITION = "eq";
	public static final String VALIDATION_NOTEQUAL_CONDITION = "ne";
	public static final String VALIDATION_GREATERTHAN_CONDITION = "g";
	public static final String VALIDATION_LESSTHAN_CONDITION = "l";
	public static final String VALIDATION_GREATERTHAN_EQUAL_CONDITION = "ge";
	public static final String VALIDATION_LESSTHAN_EQUAL_CONDITION = "le";
	public static final String VALIDATION_CONTAINS_CONDITION = "co";
	public static final String VALIDATION_DATE_FORMAT = "yyyy-MM-dd";
	public static final String VALIDATION_TIME_FORMAT = "HH:mm";
	public static final String VALIDATION_DATETIME_FORMAT = "yyyy-MM-dd HH:mm";

	// Maker-Checker Configuration
	public static final String VIEW_MAKER_CHECK = "AddRoleConfigTile";
	public static final String UPDATE_MAKER_CHECK = "updateMakerCheckerPage";
	public static final String VIEW_MAKER_CHECK_TILE = "viewMakerCheckerConfigTile";
	public static final String UPDATE_MAKER_CHECK_TILE = "updateMakerCheckerConfigTile";
	public static final String UPDATE_MAKER_RESULT = "MakerCheckerUpdateResult";
	public static final String UPDATE_MAKER_FAILED_RESULT = "MakerCheckerUpdateFailedResult";
	public static final String UPDATE_MAKER_RESULT_TILE = "successMakerCheckerConfigTile";
	public static final String OBJECT_MAKER_CHECKER = "MakerCheckerConfig";

	// Role Configuration
	public static final String ROLE_ADD_CONFIG_TILE = "AddRoleConfigTile";
	public static final String ROLE_ADD_CONFIG_ERROR = "addRoleConfigPageSearch";
	public static final String ROLE_PERMISSION_LIST = "rolePermissionList";
	public static final String PERM_MENU_TYPE = "permMenuType";
	public static final String USER_TYPE_INTERCODE = "userTypeIntercode";
	public static final String PERM_ORDER = "permOrder";
	public static final String ROLE_MENU = "m";
	public static final String ROLE_SUB_MENU = "s";
	public static final String ROLE_PERMISSION = "p";

	// Default Date Formats
	public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
	public static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";
	public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

	// Change/Reset/Forget Password
	public static final String PASS_RESET_PAGE = "getResetPasswordPage";
	public static final String PASS_RESET_TILE = "resetPasswordTile";
	public static final String PASS_RESET_SUCCESS_PAGE = "logOutAfterResetSuccess";
	public static final String PASS_CAHNGE_USERID = "userID";
	public static final String PASS_PROFILE_INFO = "passwordProfileInfo";
	public static final String USER_CURRENT_PASS = "userCurrentPassword";
	public static final String SAME_PASS_ERROR = "resetPass.error.SamePassError";
	public static final String OLD_PASS_ERROR = "resetPass.error.oldPasswordError";
	public static final String PASS_REDIRECT_KEY = "sessionRedirectKey";
	public static final String PASS_REDIRECT_VALUE = "forReset";
	public static final String ERROR_TABLE_VISIBLITY = "resetPass.error.PasswordCriteriaErrors";
	public static final String FORGOT_PASS_TILE = "forgotPasswordTile";
	public static final String FORGOT_PASS_SUCCESS_TILE = "forgotPasswordsuccessTile";
	public static final String FORGOT_PASS_USERID_ERROR = "userIdError";
	public static final String FORGOT_PASS_EMAIL_ERROR = "EmailError";
	public static final String FORGOT_PASS_NOT_ACTIVE_ERROR = "notActive";
	public static final String FORGOT_PASS_ERROR_YES = "yes";
	public static final String FORGOT_PASS_ERROR = "forGotPassword";

	public static final String ADD_ACTIVITY = "A";
	public static final String UPDATE_ACTIVITY = "U";
	public static final String DELETE_ACTIVITY = "D";
	public static final String ADD_ACTIVITY_MESSAGE = "common.button.add";
	public static final String UPDATE_ACTIVITY_MESSAGE = "common.button.update";
	public static final String DELETE_ACTIVITY_MESSAGE = "common.button.delete";

	public static final String WITH_APPROVE_YES = "Y";
	public static final String WITH_APPROVE_NO = "N";
	public static final String WITH_APPROVE_ERROR = "E";

	public static final String QUERY_CONDITION_EQUAL = "equal";
	public static final String QUERY_CONDITION_GE = "ge";
	public static final String QUERY_CONDITION_LE = "le";
	public static final String QUERY_CONDITION_GREATER = "g";
	public static final String QUERY_CONDITION_LESS = "l";
	public static final String QUERY_CONDITION_NOT_EQUAL = "notEqual";
	public static final String QUERY_CONDITION_IS_NULL = "isNULL";
	public static final String QUERY_CONDITION_NOT_NULL = "notNULL";
	public static final String QUERY_CONDITION_NOT_IN = "notIn";
	public static final String QUERY_CONDITION_IN = "in";
	public static final String QUERY_CONDITION_BETWEEN = "between";
	public static final String QUERY_WHERE_CLAUSE = " WHERE ";
	public static final String QUERY_ORDERBY_CLAUSE = " ORDER BY ";
	public static final String QUERY_INNER_JOIN = "INNER JOIN";
	public static final String QUERY_LEFT_JOIN = "LEFT JOIN";
	public static final String QUERY_RIGHT_JOIN = "RIGHT JOIN";
	public static final String QUERY_FULL_JOIN = "FULL JOIN";
	public static final String LOGICAL_OPERATOR_AND = " AND ";
	public static final String LOGICAL_OPERATOR_OR = " OR ";
	public static final String SELECT_QRY = " SELECT ";
	public static final String SELECT_COUNT = "SELECT COUNT(1) FROM ";
	public static final String FROM_QRY = " FROM ";
	public static final String DELETE_QRY = " DELETE FROM ";
	public static final String QUERY_ADD_ACTIVITY_TYPE = "addActivityType";
	public static final String QUERY_ADD_SESSION_USER_ID = "sessionUserId";
	public static final String USER_ID_FIELD = "userId";

	// PEODUCT KEY
	public static final String APPLICATION_SUPPORT_EMAIL_ID = "ni.support@concertosoft.com";
	public static final String PRODUCT_KEY_TEMPERED = "PRODUCT KEY TEMPERED";
	public static final String PRODUCT_KEY_EXPIRED = "PRODUCT LICENSE IS EXPIRED";
	public static final String PRODUCT_KEY_LIMIT = "YOU HAVE EXCEDDED LIMIT IN ";
	public static final String LICENSE_LIMIT_EXCEED = "License limit exceeded....!";
	public static final String LICENSE_ERROR_MESSAGE = " IS TEMPERED";
	public static final String LICENSE_MODULE_ERROR_MESSAGE = "MODULE DATA IS TEMPERED";
	public static final String PRODUCT_KEY_DATE = "INVALID PRODUCT_KEY DATE";
	public static final String PRODUCT_KEY_BEFORE = "PRODUCT KEY WILL BE VALID FROM";
	public static final String INVALID_PRODUCT_KEY = "Invalid product key";
	public static final String RENEW_LICENSE_LESS_VALUE_ERROR = " should not less than used";
	public static final String NOT_SUPPORTED_FILE_WARNNING_MSG = " not supported in current license but present in workflow";
	public static final String LICENSE_CHECK_FORMAT = "yyyyMMddHH";

	public static final String FAIL = "Fail";
	public static final String SUCCESS_FOR = "successFor";
	public static final String ERROR_FOR = "ErrorFor";

	public static final String SUCCESS_RESULT_WITHOUT_APPROVE = "successResultWithoutApprove";
	public static final String SUCCESS_WITHOUT_APPROVE = "successWithoutApprove";
	public static final String USER_FIRST_TIME = "FirstTime";

	public static final String ENCRYPT_ERROR = "Exception while encrypting";
	public static final String DECRYPT_ERROR = "Exception while decrypting";
	public static final String ENCODE_ERROR = "Exception while encoding";
	public static final String DECODE_ERROR = "Exception while decoding";

	// Common Result
	public static final String RESULT_PAGE_TILE = "commonResultTile";
	public static final String RESULT_PAGE_SUCCESS = "success";
	public static final String RESULT_PAGE_FAIL = "fail";
	public static final String RESULT_PAGE_APPROVE = "approve";
	public static final String RESULT_PAGE_RESULT = "result";
	public static final String RESULT_PAGE_ID = "id";
	public static final String COMMON_PAGE_ACTION = "action";
	public static final String COMMON_PAGE_REJECT_ACTION = "raction";
	public static final String COMMON_PAGE_ACTION_CLASS = "actionClass";
	public static final String RESULT_PAGE_MODULE = "module";
	public static final String RESULT_PAGE_MODULE_CLASS = "moduleClass";
	public static final String COMMON_REDIRECT_URL = "redirectURL";

	// Error Messages
	public static final String ERROR_MSG = "errorMsg";
	public static final String ERROR_TABLE_ALREADY_PRESENT_PEND = "common.message.reqPendApproval";
	public static final String ERROR_TABLE_ALREADY_PRESENT = "common.error.alreadyPresent";
	public static final String ERROR_TABLE_ASSIGN_RECORD = "common.error.assignRecord";
	public static final String ERROR_TABLE_ALREADY_PRESENT_PEND_DESC = "Request is already pending for approval";
	public static final String ERROR_TABLE_ALREADY_PRESENT_DESC = "Record is already present";
	public static final String GET_ERROR_TILE = "CommonErrorPage";
	public static final String ERROR_CODE = "code";
	public static final String ERROR_CODE_MSG = "msg";
	public static final String ERROR_CODE_DESC = "desc";
	public static final String URL_REDIRECT = "redirect";
	public static final String COMMON_ERROR_HEADER = "common.error.header";
	public static final String COMMON_ERROR_MESSAGE = "common.error.message";
	public static final String COMMON_ERROR_MESSAGE_DESC = "common.error.message.desc";
	public static final String COMMON_ACCESS_DENIED_MESSAGE = "common.access.denied.message";
	public static final String COMMON_ACCESS_DENIED_MESSAGE_DESC = "common.access.denied.message.desc";
	public static final String PREFIX_COMMON = "common.";
	public static final String SUFIX_COMMON_MESSAGE = ".message";
	public static final String SUFIX_COMMON_MESSAGE_DESC = ".message.desc";
	public static final String VISIBLE = "visible";
	public static final String DEFAULT_ERROR_MESSAGE = "System error, please try again later.";
	public static final String DEFAULT_ERROR_MESSAGE_DESC = "common.system.error";
	public static final int DEFAULT_PARAM_ERROR_STATUS = 404;
	public static final int DEFAULT_ACCESS_ERROR_STATUS = 403;
	public static final int DEFAULT_ERROR_STATUS = 500;

	// Response codes and Redirect URLs
	public static final int URL_REDIRECT_CODE = 1100;
	public static final int LOGIN_FAILED_CODE = 1000;
	public static final int SESSION_EXPIRED_CODE = 1001;
	public static final int ACCESS_DENIED_CODE = 1002;
	public static final String REDIRECT_TO_LOGIN = "/";
	public static final String REDIRECT_TO_RESET_PASS = "resetpass";
	public static final String REDIRECT_TO_HOME = "home";

	// Exception Messages
	public static final String TEMP_BEAN_ERROR = "Get temp bean error.";
	public static final String MASTER_BEAN_ERROR = "Get master bean error.";
	public static final String HIST_BEAN_ERROR = "Get hist bean error.";
	public static final String GET_PROP_VALUES_BY_CLASS = "Get property values by class error - ";
	public static final String SET_SUB_BEANS_ERROR = "Set sub beans error - ";
	public static final String SET_SUB_BEANS_CRITERIA_ERROR = "Set sub beans criteria error - ";
	public static final String GET_SUB_BEAN_FIELD_DEP = "Get sub beans field dependency error - ";
	public static final String GET_SUB_BEAN_FORM_DEP = "Get sub beans form dependency error - ";
	public static final String GET_SUB_BEAN_CLASS_NULL = "Get sub bean class is null - ";
	public static final String COMPARE_SUB_BEANS_ERROR = "Compare sub beans error - ";
	public static final String EMBEDDED_ID_CHECK_ERROR = "EmbeddedId check error - ";
	public static final String FILE_CREATE_ERROR = "File create or update error.";
	public static final String FILE_APPROVE_ERROR = "File approve error.";
	public static final String GET_FORM_KEY = "Get form bean key error - ";
	public static final String GET_UNIQUE_ID_ERROR = "Get form bean key error - ";
	public static final String PK_NEW_INSTANCE_ERROR = "Get new instance of EmbeddedId - ";
	public static final String SUB_BEAN_ID_SET_ERROR = "Sub bean ids set error - ";
	public static final String NO_SEETER_AVAIL_FOR_PROP = "No setter available for property ";
	public static final String NO_SUCH_PROP = "No such property ";
	public static final String FOR = " for ";
	public static final String EXISTS = " exists";
	public static final String ERROR_INVALID_FROM_TO_DATE_FORMAT = "Invalid from date or to date format.";
	public static final String ERROR_BEAN_INSTANCE = "Unable to create bean new instance.";
	public static final String ERROR_FORM_BEAN_INSTANCE_OR_SET_VALUES = "Unable to create form bean instance or set propert values.";
	public static final String ERROR_FIELD_VALUE_SET = "Unable set field value.";

	public static final String LINK_BASED_TYPE = "Link Based Payment Type";
	public static final String PARTIAL_LINK_FLA_GUN_CHECKED = "N";
	public static final String ADD_LINK_FLAG_OPERATION = "A";
	public static final String CARD_TOKEN_OPERATION = "Card Token Operation";

	public static final String MER_TYPE_ONE = "MerType1";
	public static final String MER_TYPE_TWO = "MerType2";
	public static final String MER_TYPE_THREE = "MerType3";

	public static final String CAPTURE = "5 - Capture";
	public static final String AUTHORIZATION = "4 - Authorization";
	public static final String VOID_AUTH = "9 - Void Authorization";
	public static final String VOID_PURCHASE = "3 - Void Purchase";
	public static final String INQUIRY = "10 - Inquiry";
	public static final String STC_PAY = "13 - STC Pay";
	public static final String TOKENIZATION = "12 - Tokenization";
	public static final String PURCHASE = "1 - Purchase";
	public static final String CREDIT = "2 - Credit";
	public static final String VOID_REFUND = "6 - Void Refund";
	public static final String SUCCESSFUL_RESP = "000";
	public static final String DECLINE = "Decline";

	public static final String ROUTING_OPTION_ALWAYS = "Always";
	public static final String ROUTING_OPTION_BISSINESS_CALL = "Bussiness Call";
	public static final String ROUTING_OPTION_PARAMETERIZE = "Parameterize";
	public static final String ROUTING_OPTION_ROUNDROBIN = "RoundRobin";
	public static final String ROUTING_OPTION_FAILOVER = "FailOver";
	public static final String ROUTING_OPTION_DEFAULT = "Default";
	public static final String ROUTING_OPTION_STANDALONE = "StandAlone";

	public static final String SETTLED = "Settled";
	public static final String PENDING = "Pending";
	public static final String REQUEST = "Request";
	public static final String ACTION = "Action";

	public static final String THREEDSECURE = "3DSecure";
	public static final String THREED_TRANSACTION_TYPE = "2";
	public static final String NONTHREEDSECURE = "NON-3DSecure";
	public static final String NON_THREED_TRANSACTION_TYPE = "1";
	public static final String PARTIAL_AMOUNT_FLAG = "Partial Amount Flag";

	// Recuring
	public static final String QUARTERLY = "QUARTERLY";
	public static final String HALF_YEARLY = "HALF YEARLY";
	public static final Object SERVICE_DOWN = "Transaction service down";
	public static final String HALFY = "HALFY";
	public static final String Q = "Q";
	public static final String DAILY = "DAILY";
	public static final String WEEKLY = "WEEKLY";
	public static final String MONTHLY = "MONTHLY";
	public static final String YEARLY = "YEARLY";
	public static final String D = "D";
	public static final String M = "M";
	public static final String W = "W";
	public static final String Y = "Y";
	public static final String N = "N";
	public static final String INSTALLMENT = "INSTALLMENT";
	public static final String SUBSCRIPTION = "SUBSCRIPTION";
	public static final String I = "i";
	public static final String S = "s";

	public static final String PRIMARY_DESTINATION = "0";
	public static final String SECONDARY_DESTINATION = "1";
	public static final String ALERT_NOTIFICATION_DESTINATION = "Destination";
	public static final String ALERT_NOTIFICATION_TRANSACTION_TYPE = "Transaction";

	public static final String FLAG_TYPE_TRANSACTION_PROF = "Transaction Profile";
	public static final String FLAG_TYPE_CURRENCY_PROF = "Currency Profile";
	public static final String FLAG_TYPE_INSTRUMENT_TYPE = "Instrument Type";
	public static final String FLAG_TYPE_VELOCITY_PROF = "Velocity Profile";
	public static final String STATUS_FOR_AUDIT = "StatusforAudit";
	public static final String SESSION_INSTITUIONID = "INSTITUIONID";
	public static final String SESSION_MERCHANTID = "MERCHANTID";
	public static final String SESSION_TERMINALID = "TERMINALID";
	public static final String SESSION_INSTITUIONID_ALLOWED = "INSTITUIONIDA";
	public static final String SESSION_MERCHANTID_ALLOWED = "MERCHANTIDA";
	public static final String SESSION_TERMINALID_ALLOWED = "TERMINALIDA";
	public static final String SESSION_BROKERID = "BROKERID";
	public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd";
	public static final String SESSION_RESPONSE_SUCCESS = "Success";
	public static final String SESSION_ERROR_FOR = "ErrorFor";
	public static final String TIME_24_HOURS_PATTERN = "([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]";
	public static final String DATE_PATTERN = "^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[012])/((19|20)\\d\\d)$";
	public static final String TRAN_FORM_BEANS = "tranFormBeans";
	public static final String NO_OF_RECURRING_PAYMENTS = "No. of Recurring Payments";
	public static final String PARAMETR_USERID = "userId";
	public static final String PARAMETER_USER_TYPE = "userType";
	public static final String COMMON_SESION_NOT_CLOSED_ERROR = "Couldn't close Session";
	public static final String CONDITION_SUCCESS = "Success";
	public static final String TRANCODE_1_PURCHASE = "1-Purchase";
	public static final String TRANCODE_2_CREDIT = "2-Credit";
	public static final String TRANCODE_3_VOID_PURCHASE = "3-Void Purchase";
	public static final String TRANCODE_4_AUTHORIZATION = "4-Authorization";
	public static final String TRANCODE_5_CAPTURE = "5-Capture";
	public static final String TRANCODE_6_VOID_REFUND = "6-Void Refund";
	public static final String TRANCODE_9_VOID_AUTHORIZATION = "9-Void Authorization";
	public static final String PAGE_NAME = "pageName";
	public static final String FINAL_STATUS = "Finalstatus";
	public static final String STRING_STATUS_FALSE = "false";
	public static final String STRING_STATUS_TRUE = "true";

	public static final String EMAIL = "EMAIL";
	public static final String SMS = "SMS";
	public static final String BOTH = "BOTH";
	public static final String TRAN_PREAUTH = "4";
	public static final String PRE_AUTH_TRANSACTION = "Pre-authorization";
	public static final String PURCHASE_TRANSACTION = "Purchase";

	public static final String SMS_MESSAGE_AR_TEMPLATE1 = "smsApi.AR.MessageStart1";

	public static final String SMS_MESSAGE_AR_TEMPLATE2 = "smsApi.AR.MessageStart2";
	public static final String SMS_MESSAGE_TEMPLATE1_TEST = "smsApi.MessageStart1_test";
	public static final String SMS_MESSAGE_AR_TEMPLATE1_TEST = "smsApi.AR.MessageStart1_test";
	public static final String SMS_USER_SENDER_CONFIG_TEST = "smsApi.smsUserSender_test";
	public static final String SMS_APPS_ID = "AppSid";
	public static final String SMS_SENDER_ID = "SenderID";
	public static final String SMS_BODY = "Body";
	public static final String SMS_RECIPIENT = "Recipient";
	public static final String SMS_APPS_ID_CONFIG = "smsApi.smsAppsID";
	public static final String SMS_SENDER_ID_CONFIG = "smsApi.smsSenderID";
	public static final String SMS_AUTH_ID_CONFIG = "smsApi.smsAUthID";
	public static final String SMS_AUTH_PASS_CONFIG = "smsApi.smsAUthPassword";
	public static final String SMS_RESPONSE_MATCH_CODE = "1";
	public static final String SMS_RESPONSE_MATCH_RESULT = "code";
	public static final String SMS_RESPONSE_MATCH_ERROR_CODE_STRING = "errorCode";
	public static final String SMS_RESPONSE_MATCH_ERROR_CODE = "ER-00";
	public static final String SMS_DATE_FORMATE = "yyyy-MM-dd";
	public static final String SMS_API_TEST_CONFIG_VALUE = "test";
	public static final String SMS_API_TEST_CONFIG_STRING = "smsAPI.test";
	public static final String SMS_USER_NAME_CONFIG = "smsApi.smsUserName";
	public static final String SMS_USER_SENDER_CONFIG = "smsApi.smsUserSender";
	public static final String SMS_MESSAGE_TEMPLATE1 = "smsApi.MessageStart1";
	public static final String SMS_MESSAGE_TEMPLATE2 = "smsApi.MessageStart2";
	public static final String SMS_API_KEY_CONFIG = "smsApi.smsApiKey";
	public static final String SMS_REQ_FILTER_CONFIG = "smsApi.smsDuplicateAllow";
	public static final String SMS_API_URL = "smsApi.url";
	public static final String SMS_USER_NAME = "userName";
	public static final String SMS_USER_SENDER = "userSender";
	public static final String FAILURE = "failure";
	public static final String SMS_NUMBERS = "numbers";
	public static final String LANGUAGE_AR = "AR";
	public static final String LANGUAGE_EN = "EN";
	public static final String PAYMENT_LINK_MODE_SMS = "0";
	public static final String PAYMENT_LINK_MODE_EMAIL = "1";
	public static final String PAYMENT_LINK_MODE_BOTH = "2";
	public static final String SHORTAN_URL_CONFIG_DOMAIN_STRING = "pg.Linkedbased.shortIo.domain";
	public static final String SHORTAN_URL_CONFIG_KEY_STRING = "pg.Linkedbased.shortIo.key";
	public static final String SHORTAN_URL_CONFIG_URL_STRING = "pg.Linkedbased.shortIo.url";
	public static final String SHORTAN_URL_ALLOW_DUPICATE_STRING = "allowDuplicates";
	public static final String SHORTAN_URL_DOMAIN_STRING = "domain";
	public static final String SHORTAN_URL_ORIGINA_URL_STRING = "originalURL";
	public static final String SHORTAN_URL_RESPONSE_MATCH_STRING = "shortURL";
	public static final String SMS_MESSAGE = "msg";
	public static final String SMS_API_KEY = "apiKey";
	public static final String SMS_REQ_FILTER = "reqFilter";
	public static final String SMS_INVOICE_PAYMENT_URL = "pg.invoice.url.sms";
	public static final String IMAGE = "image";
	public static final String BASE64_IMAGE = "BASE64_IMAGE";
	public static final String BASE64_IMAGE_FILE_PATH_KEY = "base64ImageFilePath";

	public static final String LINKBASE_EXPIRY_INTERVAL = "60";
	public static final String LINK_ISSUED_STATUS = "Issued";
	public static final String LINK_PAID_STATUS = "Paid";
	public static final String LINK_PARTIALLY_PAID_STATUS = "Partial Payment";
	public static final String LINK_EXPIRED_STATUS = "Expired";
	public static final String LINK_CANCELED_STATUS = "Canceled";
	public static final String FEEDBACK_ONLINE_USER = "FEEDBACK_ONLINE_MERCHANT";
	public static final String PROTOCOL = "https://";
	public static final String DEFAULT_MAIL_CLIENT = "VegaaH";
	public static final String DEFAULT_SAE_MAIL_CLIENT = "SAEUSA";
	public static final String CRUD = "/crud/";

	public static final String CURRENCYPROFILE = "Currency Profiles";
	public static final String FEEPROFILE = "Fee Profiles";
	public static final String TRANSACTIONPROFILE = "Transaction Profiles";
	public static final String VELOCITYPROFILE = "Velocity Profiles";

	public static final Integer ALERT_NOTIFICATION_ROUTING_CONSTANT_DAY = 7;

	public static final String TRAN_PURCHASE = "1";
	public static final String PASS_RESET_AND_LOGOUT_SUCCESSFULY = "Password Reset and Logout Successfuly";
	public static final String SESSION_EMAIL_ID = "EMAILID";
	public static final String PASS_CHANGED_SUCCESSFILLY = "password.changed.successfully";
	public static final String PAGE_ACTION = "pageAction";
	public static final int PGADMIN_USER_TYPE = 1;
	public static final int INSTITUTION_USER_TYPE = 2;
	public static final int MERCHANT_USER_TYPE = 3;
	public static final int BROKER_USER_TYPE = 4;
	public static final String INSTRUMENT_SUPPORTED_BEAN_CLASS = "InstrumentSupportedBean";
	public static final String MENU_ALLOW_BEAN_VARIABLE = "menuAllow";
	public static final String IP_RANGE = "IpRange";

	public static final String URL_CHECK = "Please check the format ot URL";

}
