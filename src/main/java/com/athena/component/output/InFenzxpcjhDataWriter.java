package com.athena.component.output;

import java.util.HashMap;
import java.util.Map;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;

public class InFenzxpcjhDataWriter  extends DBOutputTxtSerivce {

	public InFenzxpcjhDataWriter(DataParserConfig dataParserConfig) {
	}
	
	/**
	 * 运行结束后将IN_FENZXPCJH中FLAG为1的数据的FLAG值改为2
	 */
	@Override
	public void after() {
		logger.info("线程--接口" + interfaceId +" 将IN_FENZXPCJH已发送的数据状态（flag）由“1”改为“2”");
		int num = dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.updateFlagInFenzxpcjh1to2", interfaceId);
		logger.info("线程--接口" + interfaceId +" 将IN_FENZXPCJH已发送的数据状态（flag）由“1”改为“2”结束，更新"+num+"条");
	}
	
}
