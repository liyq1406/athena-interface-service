package com.athena.component.input;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;
/**
 *1781 更新订单表中订单生效时间
 * @author hzg
 * @date 2013-7-26
 */
public class UWDingdSxsjDbDataReader extends TxtInputDBSerivce{
	private String datasourceId = "";
	public UWDingdSxsjDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	/**
	 * 查询xqjs_dingd表订单号为C1DDD0009 ，用户中心为UW的记录是否存在
	 * 不存则抛出异常
	 */
	public void before(){
		try{
			String cnum = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).
			selectObject("inPutzbc.queryCountOfDingdUW");
			if("0".equals(cnum)){
				throw new ServiceException("线程--接口" + interfaceId +" xqjs_dingd表中不存在订单号为C1DDD0009，用户中心为UW的订单");
			}
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +" 查询xqjs_dingd表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"查询xqjs_dingd表时报错"+e.getMessage());
		}
	}
	

}
