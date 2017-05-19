package com.athena.component.input;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;
/**
 *2430 DDBH拆分结果
 * @author kong
 *
 */
public class ChaifjgHDBDataReader extends TxtInputDBSerivce{
	private String datasourceId = "";
	public ChaifjgHDBDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	
	/**
	 * 处理后的调用
	 */
	public void after(){
		//首先将flag为T数据也插入ddbh_chaifjginfo，再将ddbh_caifjg表flag设置为0;
        try{
        	dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.insertCaifjginfo");
        	dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.caifjgUpdate");
        }catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"更新ddbh_caifjg或ddbh_caifjginfo表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"更新ddbh_caifjg或ddbh_caifjginfo表时报错"+e.getMessage());
		}
	}

}
