package com.unistack.tamboo.message.kafka.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.rmi.ConnectException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JMX工具类,负责从JMX中获取数据
 *
 */
public class JMXHelper {
	private static final Logger logger = LoggerFactory.getLogger(JMXHelper.class);
	private String jmxUrl;

	private String username;
	private String password;

	private JMXConnector connector;
	private MBeanServerConnection connection;

	private static Map<String,JMXHelper> cache = new ConcurrentHashMap<String, JMXHelper>();
	/**
	 * 获取JMX连接实例
	 * @param jmxUrl
	 * @return
	 * @throws IOException
	 */
	public static JMXHelper getInstance(String jmxUrl) throws IOException {
		if (!cache.containsKey(jmxUrl)) {
			cache.put(jmxUrl, new JMXHelper(jmxUrl));
		}
		return cache.get(jmxUrl);
	}

	public static JMXHelper getInstance(String host, String jmxPort) throws IOException {
		return getInstance(host + ":" + jmxPort);
	}

	/**
	 * 释放JMX连接
	 * @param jmxUrl
	 * @throws IOException
	 */
	synchronized private static void refreshInstance(String jmxUrl){
		logger.warn("jmx connection of " + jmxUrl + " is broken, create a new one.");
		JMXHelper helper = null;
		try {
			helper = cache.remove(jmxUrl);
			getInstance(jmxUrl);
		} catch (Exception e) {
			logger.error("got I/O exception");
		} finally {
			if (helper != null) {
				helper.close();
			}
		}
	}

	/**
	 * 
	 * @param jmxUrl
	 * @param username
	 * @param password
	 * @throws IOException
	 */
	private JMXHelper(String jmxUrl, String username, String password) throws IOException {
		this.jmxUrl = jmxUrl;
		this.username = username;
		this.password = password;
		initConnection();
	}

	private JMXHelper(String jmxUrl) throws IOException {
		this(jmxUrl, null, null);
	}

	public String getJmxUrl() {
		return jmxUrl;
	}

	/**
	 * 初始化连接
	 * @throws IOException
	 */
	private void initConnection() throws IOException {
		String jmxURL = "service:jmx:rmi:///jndi/rmi://" + jmxUrl + "/jmxrmi";
		JMXServiceURL serviceURL = new JMXServiceURL(jmxURL);
		Map<String, String[]> map = new HashMap<String, String[]>();
		String[] credentials = null;
		if (username != null && password != null) {
			credentials = new String[] { username, password };
		}
		map.put("jmx.remote.credentials", credentials);
		try {
			connector = JMXConnectorFactory.connect(serviceURL, map);
		} catch (IOException e) {
			throw new IOException("Connection refused to host: " + jmxUrl + ", please check the broker.", e);
		}
		connection = connector.getMBeanServerConnection();
	}

	public void close() {
		try {
			if (connector != null) {
				connector.close();
			}
		} catch (IOException e) {
			logger.error("Close JMXHelper fail!", e);
		}
	}
	
	/**
	 * 获取指定objectName的数据值
	 *
	 * @param objectName The object name of the MBean from which the
	 * attribute is to be retrieved.
	 * @param attr A String specifying the name of the attribute
	 * to be retrieved.
	 *
	 * @return
	 * @throws Exception
	 */
	public Object getAttribute(ObjectName objectName, String attr) throws Exception {
		try {
			return connection.getAttribute(objectName, attr);
		} catch (ConnectException e) {
			JMXHelper.refreshInstance(jmxUrl);
			throw e;
		}
	}
	/**
	 * 查询ObjectName
	 * @param objectName
	 * @param exp
	 * @return
	 * @throws IOException
	 */
	public Set<ObjectName> queryNames(ObjectName objectName, QueryExp exp) throws IOException {
		try {
			return connection.queryNames(objectName, exp);
		} catch (ConnectException e) {
			JMXHelper.refreshInstance(jmxUrl);
			throw e;
		}
	}


	public static void main(String[] args) throws Exception {
		ObjectName objectName = new ObjectName("kafka.server:type=ZooKeeperClientMetrics,name=*,*");
		Object o = JMXHelper.getInstance("192.168.1.110:9999").getAttribute(objectName,"Max");
		System.out.println(o.toString());
	}
}
