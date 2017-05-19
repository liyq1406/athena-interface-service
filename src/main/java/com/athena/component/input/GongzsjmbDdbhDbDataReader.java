package com.athena.component.input;

import org.apache.log4j.Logger;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 3220
 * 解析数据之前的处理方法
 * @date 2014-3-20
 * @author hzg
 */
public class GongzsjmbDdbhDbDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(GongzsjmbDdbhDbDataReader.class);	//定义日志方法 
	public GongzsjmbDdbhDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	
	/**
	 * 解析数据之前清空ckx_gongzsjmb_t表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutddbh.gongzsjmbTDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + dataParserConfig.getId() +"清除ckx_gongzsjmb_t表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"清除ckx_gongzsjmb_t表时报错"+e.getMessage());
		}
	}

	/**
	 * Merge表ckx_gongzsjmb表数据
	 */
	@Override
	public void after() {
		try{
			logger.info("接口" + interfaceId +"Merge表ckx_gongzsjmb开始");
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutddbh.updateGongzsjmbCreator");
			
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutddbh.gongzsjmbTMerge");
			logger.info("接口" + interfaceId +"Merge表ckx_gongzsjmb结束");
		}catch(RuntimeException e)
		{
			logger.error("接口" + interfaceId +"Merge表ckx_gongzsjmb表时报错"+e.getMessage());
			throw new ServiceException("接口" + interfaceId +"清除ckx_gongzsjmb表时报错"+e.getMessage());
		}
	}
	
}
