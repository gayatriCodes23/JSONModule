package com.concerto.crud.common.init;

import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.ResourceBundleMessageSource;

/**
 * Copyright (C) Concerto Software and Systems (P) LTD | All Rights Reserved
 * 
 * @File : com.concerto.crud.common.init.WebMVCConfiguration.java
 * @Author : Suyog Kedar
 * @AddedDate : October 03, 2023 12:30:40 PM
 * @Purpose : Configuration class for setting up internationalization (i18n)
 *          support in a Spring MVC application. This class defines a
 *          MessageSource bean, allowing the application to handle localized
 *          messages. Messages are stored in resource bundles with a base name
 *          "i18n/message_en" for English localization.
 * @Version : 1.0
 */

@Configuration
public class WebMVCConfiguration {

	private static MessageSource messageSource;

	@Bean
	public MessageSource messageSource() {
		ResourceBundleMessageSource resourceBundleMessageSource = new ResourceBundleMessageSource();
		resourceBundleMessageSource.setBasenames("i18n/message_en");
		setMessageSource(resourceBundleMessageSource);
		return resourceBundleMessageSource;
	}

	public static MessageSource getMessageSource() {
		return messageSource;
	}

	public static void setMessageSource(MessageSource messageSource) {
		WebMVCConfiguration.messageSource = messageSource;
	}

}
