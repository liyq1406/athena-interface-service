package com.athena.component.output;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;

public class JLChaifjgDataWriter  extends DBOutputTxtSerivce {

	public JLChaifjgDataWriter(DataParserConfig dataParserConfig) {
	}
	/**
	 * 1.运行前清空In_ddbh_chaifjgxx_JL中间表
	 * 2.将DDBH拆分结果写到中间表In_ddbh_chaifjgxx_JL
	 */
	@Override
	public void before() {
		logger.info("线程--接口" + interfaceId +" 开始清空中间表In_ddbh_chaifjgxx_JL，并将ddbh_chaifjgxx_JL表数据写到中间表");	
		dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.truncateInDdbhChaifjgxxJL");
		int num = dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertInDdbhChaifjgxxJL");
		logger.info("线程--接口" + interfaceId +" 将ddbh_chaifjgxx_JL表数据写到中间表In_ddbh_chaifjgxx_JL结束，插入"+num+"条");	
		
	}
	
}
