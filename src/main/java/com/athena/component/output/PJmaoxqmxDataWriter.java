package com.athena.component.output;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;

public class PJmaoxqmxDataWriter extends DBOutputTxtSerivce {

	public PJmaoxqmxDataWriter(DataParserConfig dataParserConfig) {
	}
	/**
	 * 1.运行前清空In_ddbh_chaifjgxx_JL中间表
	 * 2.将DDBH拆分结果写到中间表In_ddbh_chaifjgxx_JL
	 */
	@Override
	public void before() {
		logger.info("线程--接口" + interfaceId +" 开始清空中间表in_ddbh_maoxqmx，并将ddbh_maoxqmx表中的FLAG状态为0的改为1");	
		dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.truncateInDdbhMaoxqmx");
		dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.updateFlagInDdbhMaoxqmx0to1");
		int num = dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertInDdbhMaoxqmx");
		logger.info("线程--接口" + interfaceId +" 将ddbh_maoxqmx表数据写到中间表in_ddbh_maoxqmx结束，插入"+num+"条");
	}
	
	/**
	 * 运行结束，将ddbh_maoxqmx表中的FLAG状态为1的改为2
	 */
	@Override
	public void after() {
		logger.info("线程--接口" + interfaceId +" 将ddbh_maoxqmx表中的FLAG状态为1的改为2");
		dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.updateFlagInDdbhMaoxqmx1to2");
	}
	
	
	
}
