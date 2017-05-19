package com.athena.component.service.imp;

import java.util.List;

import javax.jws.WebService;

import org.apache.log4j.Logger;

import com.athena.component.service.BaseService;
import com.athena.component.service.InterfaceCASCwebService;
import com.athena.component.service.bean.Baoz;
import com.athena.component.service.bean.Gysckbaoz;
import com.athena.component.service.bean.Lingjck;
import com.athena.component.service.bean.Lingjgys;
import com.athena.db.ConstantDbCode;
import com.toft.core3.container.annotation.Component;


/**
 * webservice实现类，供外部系统调用
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2015-7-21
 */
@WebService(endpointInterface="com.athena.component.service.InterfaceCASCwebService",serviceName="/interfaceCASCwebService")
@Component
public class InterfaceCASCwebServiceImpl extends BaseService  implements InterfaceCASCwebService {
	// log4j日志初始化
	static Logger logger = Logger.getLogger(InterfaceCASCwebServiceImpl.class); 
	/**
	 * 返回包装结果集
	 */
	@SuppressWarnings("unchecked")
	@Override
	public  Gysckbaoz getLingjGysCkBaozBean(String lingjbh) {
		Gysckbaoz gysckbaoz = new Gysckbaoz(); 
		//零件供应商信息
		List<Lingjgys> ljgysList = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CKX).select("common.queryLingjgysJk", lingjbh);
		//零件仓库信息
		List<Lingjck> ljckList = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CKX).select("common.queryLingjckJk", lingjbh);;
		//零件包装信息
		List<Baoz> baozList = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CKX).select("common.queryBaozJk", lingjbh);;
		//封装结果集
		gysckbaoz.setLingjgysList(ljgysList);
		gysckbaoz.setLingjckList(ljckList);
		gysckbaoz.setBaozList(baozList);
		//根据零件编号查询零件供应商包装信息
		/*List<Gysckbaoz> gysList = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CKX).select("common.queryLingjgysBaoz", lingjbh);
		for(Gysckbaoz gysbz : gysList){
			gysckbaoz.add(gysbz);
		}
		//根据零件编号查询零件仓库包装信息
		List<Gysckbaoz> ckList = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CKX).select("common.queryLingjckBaoz", lingjbh);
		for(Gysckbaoz ckbz : ckList){
			gysckbaoz.add(ckbz);
		}*/
		return gysckbaoz;
	}

}
