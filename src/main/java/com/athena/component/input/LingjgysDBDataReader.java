package com.athena.component.input;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 2210 零件供应商 
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2014-2-27
 */
public class LingjgysDBDataReader extends TxtInputDBSerivce {

	private String datasourceId = "";
	public LingjgysDBDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	/**
	 * 2210输入完成后 ，1.更新ck_lingjgys表中A、B点供应商表达总量
	 *                2.将ckx_lingjgys表中的flag状态1改为0
	 */
	@Override
	public void after(){
        try{
        	dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.updateShulOfCkLingjgys");
        	dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.updateFlagOfLingjgys");
        }catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"更新执行层零件供应商表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"更新执行层零件供应商表时报错"+e.getMessage());
		}
	}

}
