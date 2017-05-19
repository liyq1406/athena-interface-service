package com.athena.component.service.utls;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import com.athena.component.exchange.ParserException;
import com.athena.component.service.bean.InterfaceConfig;
import com.athena.component.service.bean.OrderConfig;

public class InterFaceParserXmlHelper {
	private static final Log logger = LogFactory.getLog(InterFaceParserXmlHelper.class);
	
	public static Document readFromXml(InputStream in) {
		SAXReader reader = new SAXReader();
		Document doc = null;
		try {
			doc = reader.read(in);
		} catch (DocumentException e) {
			throw new ParserException("配置文件读取异常："+e.getMessage());
		}
		return doc;
	}
	
	/**
	 * 
	 * @param doc
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static Map<? extends String, ? extends InterfaceConfig> parseConfigs(
			Document doc) {
		Map<String,InterfaceConfig> configs = new HashMap<String,InterfaceConfig>();
		
		if(doc!=null){
			List<Element> interfaceElements = doc.selectNodes("interfaces/interface");
			for(Element interElement:interfaceElements){
				InterfaceConfig inter = new InterfaceConfig();
				
				inter.setId(interElement.attributeValue("id"));
				inter.setName(interElement.attributeValue("name"));
				inter.setType(interElement.attributeValue("type"));
				
				inter.setOrders(parseOrderConfigs(interElement));
				configs.put(inter.getId(), inter);
			}
		}
		logger.info("读取数据交换配置完成！");
		
		return configs;
	}
	
	/**
	 * 
	 * @param interElement
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static List<OrderConfig> parseOrderConfigs(Element interElement) {
		 List<OrderConfig> orderConfigs = new ArrayList<OrderConfig>();
		 
		 if(interElement!=null){
			 List<Element> orderElements = interElement.selectNodes("order"); 
			 for(Element orderElement:orderElements){
					OrderConfig orderconfig = new OrderConfig();
					orderconfig.setId(orderElement.attributeValue("id"));
					orderconfig.setName(orderElement.attributeValue("name"));
					orderconfig.setIstxt(orderElement.attributeValue("istxt"));
					orderconfig.setFlag(orderElement.attributeValue("flag"));
					orderconfig.setIsDb(orderElement.attributeValue("isDb"));
					orderconfig.setSort(orderElement.attributeValue("sort"));
					orderConfigs.add(orderconfig);
				}
		 }
		 
		return orderConfigs;
	}

}
