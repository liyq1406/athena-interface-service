package com.athena.component.service;

import java.util.List;

import javax.jws.WebParam;
import javax.jws.WebService;

import com.athena.component.service.bean.ServiceBean;

@WebService
public interface InterfaceService {

	public void setServiceBean(@WebParam(name="ServiceBean")List<ServiceBean> list);
}
