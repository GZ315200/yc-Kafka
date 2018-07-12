package com.unistack.tamboo.message.kafka.errors;
/**
 * 配置文件找不到异常 
 *
 */
public class ConfigNotFoundException extends RuntimeException {
	private static final long serialVersionUID = -3199832436338918089L;

	public ConfigNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}

	public ConfigNotFoundException(String message) {
		super(message);
	}

	public ConfigNotFoundException(Throwable cause) {
		super(cause);
	}

	public ConfigNotFoundException() {
		super();
	}

}
