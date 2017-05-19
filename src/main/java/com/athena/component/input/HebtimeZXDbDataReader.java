package com.athena.component.input;

import org.apache.log4j.Logger;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 2860 合并时间-详细
 * 解析数据之前的处理方法
 * @date 2014-3-20
 * @author gswang
 */
public class HebtimeZXDbDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(HebtimeZXDbDataReader.class);	//定义日志方法 
	public HebtimeZXDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	
	/**
	 * 解析数据之前将CKX_CALENDAR_HEBTIME_INFO表中字段creator值更新为temp
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.HebtimeUpdateTemp");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + dataParserConfig.getId() +"清除ckx_calendar_hebtime_info表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"清除ckx_calendar_hebtime_info表时报错"+e.getMessage());
		}
	}

	/**
	 * 1、执行完成后删除CKX_GONGZSJMB表中字段creator为temp的记录，即不是本次更新的数据
	 * 2、执行完成后删除ckx_calendar_hebtime_info表中字段creator为temp的记录
	 * 
	 */
	@Override
	public void after() {
		try{
			logger.info("接口" + interfaceId +"表ckx_calendar_hebtime_info开始");
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.deleteGongzsjmbTemp");
			
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.HebtimeZXCDeleteTemp");
			logger.info("接口" + interfaceId +"表ckx_calendar_hebtime_info结束");
		}catch(RuntimeException e)
		{
			logger.error("接口" + interfaceId +"Merge表ckx_calendar_hebtime_info表时报错"+e.getMessage());
			throw new ServiceException("接口" + interfaceId +"清除ckx_calendar_hebtime_info表时报错"+e.getMessage());
		}
	}
	
}
