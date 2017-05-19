package com.athena.component.input;

import org.apache.log4j.Logger;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 3210  合并时间-详细
 * 解析数据之前的处理方法
 * @date 2014-3-20
 * @author gswang
 */
public class HebtimeDdbhDbDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(HebtimeDdbhDbDataReader.class);	//定义日志方法 
	public HebtimeDdbhDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	
	/**
	 * 解析数据之前更新ckx_calendar_hebtime_info表数据
	 */
	@Override
	public void before() {
		try{
			logger.info("接口" + interfaceId +" 执行ckx_calendar_hebtime_info表update开始");
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutddbh.HebtimeDDBHUpdateTemp");
			logger.info("接口" + interfaceId +" 执行ckx_calendar_hebtime_info表update结束");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"更新ckx_calendar_hebtime_info表creator为temp时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"更新ckx_calendar_hebtime_info表creator为temp时报错"+e.getMessage());
		}
	}

	/**
	 * 1、执行完成后删除CKX_GONGZSJMB表中字段creator为temp的记录，即不是本次更新的数据
	 * 2、ckx_calendar_hebtime_info表数据更新完成后，删除掉不是本次更新的数据
	 */
	@Override
	public void after() {
		try{
			logger.info("接口" + interfaceId +"表ckx_calendar_hebtime_info开始");
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutddbh.deleteGongzsjmbTemp");
			
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutddbh.HebtimeDDBHDeleteTemp");
			logger.info("接口" + interfaceId +"表ckx_calendar_hebtime_info结束");
		}catch(RuntimeException e)
		{
			logger.error("接口" + interfaceId +"Merge表ckx_calendar_hebtime_info表时报错"+e.getMessage());
			throw new ServiceException("接口" + interfaceId +"清除ckx_calendar_hebtime_info表时报错"+e.getMessage());
		}
	}
	
}
