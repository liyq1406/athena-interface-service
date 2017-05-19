package com.athena.component.input;

import org.apache.log4j.Logger;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 2016-01-26
 */
public class InLcdvClddxxDbDataWriter extends TxtInputDBSerivce {
	protected static Logger logger = Logger.getLogger(InLcdvClddxxDbDataWriter.class);	//定义日志方法
	private String datasourceId = "";
	
	public InLcdvClddxxDbDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId=dataParserConfig.getReaderConfig().getDatasourceId();
	}

	/**
	 * 数据插入完毕后，如果in_clddxx表中有空lcdv的情况，则将in_lcdv_clddxx表中usercenter和whof对应数据的lcdv和lcdvbzk插入in_clddxx表中
	 */
	@Override
	public void after() {
		try{	
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.updateILCtoIC");
		}catch(Exception e){
			logger.error("线程--接口" + dataParserConfig.getId() +"更新数据表in_clddxx报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"更新数据表in_clddxx报错"+e.getMessage());
		}
	}
	
}
