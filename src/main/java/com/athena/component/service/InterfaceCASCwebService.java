package com.athena.component.service;

import javax.jws.WebParam;
import javax.jws.WebService;

import com.athena.component.service.bean.Gysckbaoz;

/**
 * CASC webservic接口调用
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2015-7-21
 */
@WebService
public interface InterfaceCASCwebService {
	public Gysckbaoz getLingjGysCkBaozBean(@WebParam(name="lingjbh") String lingjbh);
}
