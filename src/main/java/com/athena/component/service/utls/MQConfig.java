package com.athena.component.service.utls;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


/**
 * 读取配置文件
 * @version v1.0
 * @date 2016-1-15
 */
public class MQConfig {
	private static MQConfig instance = null;

	private String MQ_MANAGER = null;
	private String MQ_HOST_NAME = null;
	private String MQ_CHANNEL = null;
	private String MQ_QUEUE_NAME = null;
	private String MQ_PROT = null;
	private String MQ_CCSID = null;
	private String MQ_QUEUE_SUB = null;
	private String FILE_DIR = null;
	private String shellName = null;

	

	public static MQConfig getInstance() {
		if (instance == null) {
			instance = new MQConfig();
		}
		return instance;
	}

	public static MQConfig getNewDbConfigFromKey() {
		MQConfig dc = null;
		Properties prop = new Properties();
		InputStream fis = null; 
		try {
			fis = MQConfig.class.getClassLoader().getResourceAsStream("config/exchange/config.properties");
			prop.load(fis);
			dc = new MQConfig();
			dc.MQ_MANAGER = prop.getProperty("MQ_MANAGER");
			dc.MQ_CCSID = prop.getProperty("MQ_CCSID");
			dc.MQ_CHANNEL = prop.getProperty("MQ_CHANNEL");
			dc.MQ_HOST_NAME = prop.getProperty("MQ_HOST_NAME");
			dc.MQ_PROT = prop.getProperty("MQ_PROT");
			dc.MQ_QUEUE_NAME = prop.getProperty("MQ_QUEUE_NAME");
			dc.MQ_QUEUE_SUB = prop.getProperty("MQ_QUEUE_SUB");
			dc.FILE_DIR = prop.getProperty("FILE_DIR");
		} catch (FileNotFoundException e) {
			dc = null;
		} catch (IOException e) {
			dc = null;
		}
		return dc;
	}
	
	public String getShellName() {
		return shellName;
	}

	public void setShellName(String shellName) {
		this.shellName = shellName;
	}

	public String getFILE_DIR() {
		return FILE_DIR;
	}

	public void setFILE_DIR(String fILE_DIR) {
		FILE_DIR = fILE_DIR;
	}

	public String getMQ_MANAGER() {
		return MQ_MANAGER;
	}

	public void setMQ_MANAGER(String mq_manager) {
		MQ_MANAGER = mq_manager;
	}

	public String getMQ_HOST_NAME() {
		return MQ_HOST_NAME;
	}

	public void setMQ_HOST_NAME(String mq_host_name) {
		MQ_HOST_NAME = mq_host_name;
	}

	public String getMQ_CHANNEL() {
		return MQ_CHANNEL;
	}

	public void setMQ_CHANNEL(String mq_channel) {
		MQ_CHANNEL = mq_channel;
	}

	public String getMQ_QUEUE_NAME() {
		return MQ_QUEUE_NAME;
	}

	public void setMQ_QUEUE_NAME(String mq_queue_name) {
		MQ_QUEUE_NAME = mq_queue_name;
	}

	public String getMQ_PROT() {
		return MQ_PROT;
	}

	public void setMQ_PROT(String mq_prot) {
		MQ_PROT = mq_prot;
	}

	public String getMQ_CCSID() {
		return MQ_CCSID;
	}

	public void setMQ_CCSID(String mq_ccsid) {
		MQ_CCSID = mq_ccsid;
	}	
	
	public void setMQ_QUEUE_SUB(String mQ_QUEUE_SUB) {
		MQ_QUEUE_SUB = mQ_QUEUE_SUB;
	}

	public String getMQ_QUEUE_SUB() {
		return MQ_QUEUE_SUB;
	}
	
}
