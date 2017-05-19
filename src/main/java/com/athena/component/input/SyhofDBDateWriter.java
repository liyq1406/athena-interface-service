package com.athena.component.input;

import java.sql.SQLException;

import org.apache.log4j.Logger;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 3010 商业化OF接口
 * @date 2013-3-15
 * @author hzg
 *
 */
public class SyhofDBDateWriter extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(SyhofDBDateWriter.class);	//定义日志方法
	private String datasourceId = "";
	
	public SyhofDBDateWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId=dataParserConfig.getReaderConfig().getDatasourceId();
	}
	
	
	/**
	 * 执行前将sppv.ATHENA002表中的JSBS='F'改为'J' hzg  
	 * 2013-12-4
	 */
	@Override
	public void before(){
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.updateATHENA002JsbsToJ");
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"before更新sppv.ATHENA002表JSBS状态F为J时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"before更新sppv.ATHENA002表JSBS状态F为J时报错"+e.getMessage());
		}
	}
	
	
	/**
	 * 接口处理完成后更新SPPV ATHENA002表的状态为'T'(已处理状态)
	 * @throws SQLException 
	 */
	@Override
	public void after() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.updateAthena002");
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"更新ATHENA002表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"更新ATHENA002表时报错"+e.getMessage());
		}
	}

}
