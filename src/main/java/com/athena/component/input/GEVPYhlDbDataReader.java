package com.athena.component.input;

import org.apache.log4j.Logger;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 2030
 * 解析数据之前的处理方法
 * @date 2013-3-6
 * @author hzg
 */
public class GEVPYhlDbDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(GEVPYhlDbDataReader.class);	//定义日志方法
	private String datasourceId = "";
	public GEVPYhlDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();

	}
	
	
	/**
	 * 解析数据之前清空GEVP要货令表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.gevpYaohlDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + dataParserConfig.getId() +"清除in_gevp_yaohl表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"清除in_gevp_yaohl表时报错"+e.getMessage());
		}
	}
	 
	/**
	 * 行解析之前处理方法
	 */
	@Override
	public boolean beforeRecord(String line, String fileName, int lineNum){
		boolean result = true;
		if(line.toString().startsWith("ENROB")){
			result = false;
		}
		return result;
	}

	@Override
	public void after() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.updateGevpYaohlUsercenter");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + dataParserConfig.getId() +"更新in_gevp_yaohl表要货用户中心出错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"更新in_gevp_yaohl表要货用户中心出错"+e.getMessage());
		}
	}
}
