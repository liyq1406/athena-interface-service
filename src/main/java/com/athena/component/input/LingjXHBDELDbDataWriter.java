package com.athena.component.input;

import org.apache.log4j.Logger;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 1170 异常申报
 * @author hzg
 *
 */
public class LingjXHBDELDbDataWriter extends TxtInputDBSerivce{
	
	protected static Logger logger = Logger.getLogger(LingjXHBDELDbDataWriter.class);	//定义日志方法
	
	public LingjXHBDELDbDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig); 
	}
	

	/**
	 * 数据解析之前清空零件XQJS_KUCJSCSB表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzbc.ycsbDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除XQJS_KUCJSCSB表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除XQJS_KUCJSCSB表时报错"+e.getMessage());
		}
	}
	
}
