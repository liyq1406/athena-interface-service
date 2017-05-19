package com.athena.component.output;
import java.util.List;
import java.util.Map;

import com.athena.util.exception.ServiceException;
import com.toft.core3.ibatis.support.AbstractIBatisDao;

/**
 * 2120库存快照
 * @author hzg
 * @date 2013-7-29
 *
 */

public class KuckzTxtWriter extends  DuoJieKouOutputWriter{
	public KuckzTxtWriter() {
		
	}

	/**
	 * 解决文件之前，查询库存快照需要的数据并写入到ck_kuckz表中，SQL为胡雪宜提供
	 * @throws RuntimeException 
	 */
	@SuppressWarnings("unchecked")
	public void beforeExecute2120(AbstractIBatisDao baseDao,String sourceId) {
		try{
			//1、执行之前清空表记录
			baseDao.getSdcDataSource(sourceId).execute("outPut.deleteKuckz");
			//2、判断库存快照资源设置表中是否存在生效标识SHENGXBS为1的数据   2013-10-14根据用户中心写数据
			//2.1、取ck_usbq表用户中心
			List<Map<String,String>> UCMap =  baseDao.getSdcDataSource(sourceId).select("outPut.usbqDisdinctQuery");
			for(Map<String,String> map : UCMap){  //循环用户中心 2013-10-14 hzg
				String c = (String)baseDao.getSdcDataSource(sourceId).selectObject("outPut.queryCountKckzsz",map.get("USERCENTER"));
				if("0".equals(c)){ //不存在，取ck_usbq数据不区分baozztt和zhuangtsx
					baseDao.getSdcDataSource(sourceId).execute("outPut.insertKuckzALL_1",map.get("USERCENTER"));
				}else{ //取shengxbs = '1'的数据 区分baozztt和zhuangtsx
					baseDao.getSdcDataSource(sourceId).execute("outPut.insertKuckz_1",map.get("USERCENTER"));
				}
				baseDao.getSdcDataSource(sourceId).execute("outPut.insertKuckz_2",map.get("USERCENTER"));
			}
			baseDao.getSdcDataSource(sourceId).execute("outPut.insertKuckzALL_2");
		}catch(RuntimeException e)
		{
			logger.error("接口" + interfaceId +"插入ck_kuckz表时报错"+e.getMessage());
			throw new ServiceException("接口" + interfaceId +"插入ck_kuckz表时报错"+e.getMessage());
		}
	}

}
   