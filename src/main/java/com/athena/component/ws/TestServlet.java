package com.athena.component.ws;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.cxf.frontend.ClientProxy;
import org.apache.cxf.frontend.ClientProxyFactoryBean;
import org.apache.cxf.transport.Conduit;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.apache.log4j.Logger;

import com.athena.component.service.DispatchService;

/**
 * 测试的servlet 调用webService
 * @author chenlei
 * @vesion 1.0
 * @date 2012-4-25
 */
public class TestServlet extends HttpServlet {
	protected static Logger logger = Logger.getLogger(TestServlet.class);	//定义日志方法
	/**
	 * 序列号
	 */
	private static final long serialVersionUID = 1L;

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		doPost(req, resp);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		//得到请求路径
		String path = req.getRequestURL().toString();
		path = path.substring(0, path.length()-5)+"/services/dispatchServiceImpl";
	
		//处理用上传的参数  根据参数 执行相应的接口方法
		String interFaceName = req.getParameter("interFaceType");
		try{
			doInterFace(interFaceName,path);
		}catch(Exception e){
			logger.error("接口出错doPost:"+interFaceName);
		}
		//跳转
		req.getRequestDispatcher("index.jsp").forward(req, resp);
		
	}

	/**
	 * 执行接口相应接口方法
	 * @param interFaceName
	 */
	public void doInterFace(String interFaceName,String path){
//		JaxWsProxyFactoryBean factory = new JaxWsProxyFactoryBean();
//        factory.setAddress(path);
//        DispatchService ds = factory.create(DispatchService.class);
//		ds.dispatchTask(interFaceName);
		long starttime=System.currentTimeMillis(); 
		ClientProxyFactoryBean factory = new ClientProxyFactoryBean();
		factory.setServiceClass(DispatchService.class);
		factory.setAddress(path);
		DispatchService client = (DispatchService) factory.create();
		
		//设置连接超时
		recieveTimeOutWrapper(client);
		int i =0;	
		try{
			i = client.dispatchTask(interFaceName);
		}catch(Exception e){
			logger.error("接口出错了doInterFace:"+interFaceName);
		}

		//jieshishijian
		System.out.println("耗时"+(System.currentTimeMillis()-starttime)+"毫秒,状态： "+ i);
		
	}
	
	/**
	 * 连接时间为5分钟
	 * @param o
	 */
	 private  void recieveTimeOutWrapper(Object o)  
	 {  
	     Conduit conduit = (ClientProxy.getClient(o).getConduit());  
	     HTTPConduit hc = (HTTPConduit)conduit;  
	     HTTPClientPolicy client = new HTTPClientPolicy();  
	     // 5分钟超时时间
	     client.setReceiveTimeout(1000 * 60 * 5);    
	     hc.setClient(client);  
	 }

}
