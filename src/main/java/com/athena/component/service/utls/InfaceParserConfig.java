package com.athena.component.service.utls;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.DataParserConfigFactory;
import com.athena.component.exchange.config.DataParserXmlHelper;
import com.athena.component.service.bean.InterfaceConfig;

/**
 * 解析器 解析exchange-inface.xml
 * @author chenlei
 * @vesion 1.0
 * @date 2012-5-25
 */
public class InfaceParserConfig {
	private static final Log logger = LogFactory.getLog(InfaceParserConfig.class);
	
	private Map<String,InterfaceConfig> interFaceParserConfigs =null ;
	
	private static final InfaceParserConfig dpcf = 
		new InfaceParserConfig();
	
	/*
	 * 初始化方法
	 */
	public void init(){
		loadConfig();
	}
	
	private InfaceParserConfig(){
		init();
	}
	public static InfaceParserConfig getInstance(){
		return dpcf;
	}
	
	/**
	 * 加载配置文件
	 */
	private void loadConfig(){
		clear();
		URL rul = InfaceParserConfig.class.getClassLoader().getResource("config/exchange/exchange-interface.xml");
		
		InputStream in =null;
		try {
			in = rul.openStream();
		} catch (IOException e) {
			//记录日志 系统级异常
		}
		
		Document doc =
			InterFaceParserXmlHelper.readFromXml(in);
		interFaceParserConfigs = (Map<String, InterfaceConfig>) InterFaceParserXmlHelper.parseConfigs(doc);
		logger.info("成功加载数据交换配置文件.");
	}
	
	public InterfaceConfig getDataParserConfig(String configId){
		return interFaceParserConfigs.get(configId);
	}
	
	public void clear(){
		if(interFaceParserConfigs!=null){
			interFaceParserConfigs.clear();
		}
		
	}
}
