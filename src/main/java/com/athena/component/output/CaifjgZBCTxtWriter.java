package com.athena.component.output;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;

/**
 * 3150 DDBH拆分结果写中间表
 * @author hzg
 * @date 2015-5-11
 *
 */

public class CaifjgZBCTxtWriter extends  DBOutputTxtSerivce{
	public CaifjgZBCTxtWriter(DataParserConfig dataParserConfig) {
	}

	/**
	 * 1.运行前清空IN_SPPV_CAIFJGXX中间表
	 * 2.将DDBH拆分结果写到中间表IN_SPPV_CAIFJGXX
	 */
	@Override
	public void before() {
		logger.info("线程--接口" + interfaceId +" 开始清空中间表IN_SPPV_CAIFJGXX，并将ddbh_chaifjgxx表数据写到中间表");	
		dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.truncateInSppvCaifjg");
		int num = dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertInSppvCaifjg");
		logger.info("线程--接口" + interfaceId +" 将ddbh_chaifjgxx表数据写到中间表IN_SPPV_CAIFJGXX结束，插入"+num+"条");	
		
	}

}
   