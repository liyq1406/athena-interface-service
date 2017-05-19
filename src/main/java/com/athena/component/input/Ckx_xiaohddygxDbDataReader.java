package com.athena.component.input;



import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;




/**
 * 1121 消耗点对应关系
 * @date 2015-12-31
 * @author yz
 */
public class Ckx_xiaohddygxDbDataReader extends TxtInputDBSerivce{
	private String datasourceId = "";

	public Ckx_xiaohddygxDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	


	/**
	 * 接口运行后处理方法
	 * 更新工艺消耗点距上线点车身数
	 */
	@Override
	public void after() {
			try{		
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzbc.updateckx_gongyxhd");
			}catch(RuntimeException e){
				logger.error("线程--接口" + interfaceId +"更新时报错"+e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId +"插入时报错"+e.getMessage());
			}		
		
	}	
	
}
