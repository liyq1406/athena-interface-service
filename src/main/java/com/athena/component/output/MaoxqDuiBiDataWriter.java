package com.athena.component.output;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;

public class MaoxqDuiBiDataWriter extends DBOutputTxtSerivce {

	public MaoxqDuiBiDataWriter(DataParserConfig dataParserConfig) {
	} 
	
	/**
	 * 更新数据表状态
	 */
	@Override
	public void after() {
		logger.info("线程--接口" + interfaceId +" 将周期毛需求对比表中的状态改为1");
		dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.updatePPMaoxqDuibi");		
		dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.updatePPMaoxqDuibi_Mx");
		
		logger.info("线程--接口" + interfaceId +" 将PPL毛需求对比表中的状态改为1");
		dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.updatePPLMaoxqDuibi");
		dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.updatePPLMaoxqDuibi_Mx");
		
	}
	
	
	
}
