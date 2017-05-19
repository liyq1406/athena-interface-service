package com.athena.component.output;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;


/**
 * 3440 小火车消耗点模板(输出)
 * @author lc
 * @date 2015-12-14
 */

public class XiaohcxhdmbTxtWriter extends  DBOutputTxtSerivce{
	public XiaohcxhdmbTxtWriter(DataParserConfig dataParserConfig) {
	}

	/**
	 * 1.运行前清空ddbh_xiaohcxhdmb表
	 * 2.将数据插入到ddbh_xiaohcxhdmb表
	 */
	@Override
	public void before() {
		logger.info("线程--接口" + interfaceId +" 开始清空ddbh_xiaohcxhdmb表，并将数据插入到ddbh_xiaohcxhdmb表");	
		dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.truncateddbhxiaohcxhdmb");
		int num = dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.insertddbhxiaohcxhdmb");
		logger.info("线程--接口" + interfaceId +" 将数据插入到ddbh_xiaohcxhdmb表结束，插入"+num+"条");	
		
	}

}
   