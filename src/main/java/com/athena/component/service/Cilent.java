package com.athena.component.service;



import java.util.ArrayList;
import java.util.List;

import org.apache.cxf.frontend.ClientProxyFactoryBean;

import com.athena.component.service.bean.ServiceBean;

public class Cilent {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		ClientProxyFactoryBean factory = new ClientProxyFactoryBean();
//		factory.setServiceClass(DispatchService.class);
//		factory.setAddress("http://localhost:8096/athena-interface-service/services/dispatchServiceImpl");
//		DispatchService client = (DispatchService) factory.create();
//		client.dispatchTask("IN_1");
		
		
		
		ClientProxyFactoryBean factory = new ClientProxyFactoryBean();
		factory.setServiceClass(InterfaceService.class);
		factory.setAddress("http://localhost:8096/athena-interface-service/services/InterfaceServiceImp");
		InterfaceService client = (InterfaceService) factory.create();	
		List<ServiceBean> list=new ArrayList<ServiceBean>();
		ServiceBean bean=new ServiceBean();
		bean.setDbsqdh("02145421");
		bean.setDbsq_date("2011-03-22");
		bean.setKjkm("abcdef");
		bean.setCbzx("WUS");
		bean.setZzlx("FWS");
		bean.setLjh("1212412214");
		bean.setSbsl(12000);
		bean.setUsercenter("UW");		
     	list.add(bean);
		client.setServiceBean(list);
	}

}
