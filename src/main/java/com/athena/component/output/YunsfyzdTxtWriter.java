package com.athena.component.output;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;


/**
 * 1990运输费用主单输出
 * @author LC
 * @date 2017-04-07
 */
public class YunsfyzdTxtWriter extends DBOutputTxtSerivce {
	public YunsfyzdTxtWriter(DataParserConfig dataParserConfig) {
	}

	/**
	 * 运行前先将ck_yunsfyhz表中faszt为0（未发送）的数据状态更新为1（发送中）
	 */
	@Override
	public void before() {
		logger.info("线程--接口" + interfaceId +" 更新ck_yunsfyhz表状态");
		int num = dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.updateyunsfyzdBegin");
		logger.info("线程--接口" + interfaceId +" 更新ck_yunsfyhz表状态结束，共更新"+num+"条");
	}
    

	/**
	 * 运行后将ck_yunsfyhz表中faszt为1（发送中）的数据状态更新为2（已发送）
	 */
	@Override
	public void after() {
		logger.info("线程--接口" + interfaceId +" 更新ck_yunsfyhz表状态");
		int num = dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.updateyunsfyzdEnd");
		logger.info("线程--接口" + interfaceId +" 更新ck_yunsfyhz表状态结束，共更新"+num+"条");	
	}

}
   