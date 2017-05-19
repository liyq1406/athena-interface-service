package com.athena.component.service;

import javax.jws.WebService;

@WebService
public interface IKdysService {

	/**
	 * 调用Kdys
	 * @date 2012-9-28
	 * @throws ServiceException
	 */
	public String executeKdysService();
	
}
