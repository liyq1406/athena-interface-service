package com.athena.component.service.intercetpr;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.cxf.binding.soap.SoapMessage;
import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.phase.AbstractPhaseInterceptor;
import org.apache.cxf.transport.http.AbstractHTTPDestination;

public class MyIntercetpr extends AbstractPhaseInterceptor<SoapMessage> {
	protected final Log logger = LogFactory.getLog(getClass());// 定义日志方法

	public MyIntercetpr(String phase) {
		super(phase);
	}

	@Override
	public void handleMessage(SoapMessage message) throws Fault {
		 	String[] object=null;
		 	boolean falg=false;
		 	HttpServletRequest request = (HttpServletRequest) message.get(AbstractHTTPDestination.HTTP_REQUEST);  
		 	String ip = request.getRemoteAddr();
		 	object=getProperties().split(" ");
		 	if(null!=ip&&!"".equals(ip)){
		 	for (int i = 0; i < object.length; i++) {
		 		//将请求的ip与文件中的ip匹配,如果匹配正确
				if(ip.equals(object[i])){
					falg=true;
					break;
				}	
			}
		 	//如果匹配不上的 就抛出此异常
		 	if(!falg){
		 		throw new Fault(new IllegalAccessException(ip + "访问被终止"));
		 	}
		 }
	}
	
	
   /**
    * 读取properties文件IP的信息	
    * @return String
    */
   public String getProperties()
   {
	   String ip=null;
	   Properties pro=null;
	   InputStream inStream= MyIntercetpr.class.getResourceAsStream("/properties/ipConfig.properties");
	   pro=new Properties();
	   if(null==inStream){
		   logger.info("配置文件不存在，请检查文件名或路径是否正确!");
		   return null;
	   }
	   try {
		pro.load(inStream);
		if(null!=pro){
		ip=pro.getProperty("ip");
		}
	} catch (IOException e) {
		logger.info("配置文件加载失败，请检查文件名或路径是否正确!"+e.getMessage());
		return null;
	}finally{
		if(null!=inStream){
			try {
				inStream.close();
			} catch (IOException e) {
				logger.info("配置文件文件流关闭失败!"+e.getMessage());
				return null;
			}
		}
	}
	   return ip;
   }
}



