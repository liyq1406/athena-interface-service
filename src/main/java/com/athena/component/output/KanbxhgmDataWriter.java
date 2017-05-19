package com.athena.component.output;

import java.util.HashMap;
import java.util.Map;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;

public class KanbxhgmDataWriter extends DBOutputTxtSerivce{
	public KanbxhgmDataWriter(DataParserConfig dataParserConfig) {
		
	}

	public void afterAllRecords(ExchangerConfig[] ecs) {
		Map<String,String> param = new HashMap<String,String>();
		baseDao.getSdcDataSource(sourceId).execute("outPut.kanbxhgmDataUpdate",param);
		baseDao.getSdcDataSource(sourceId).execute("outPut.kanbxhgmDataShix",param);
		super.afterAllRecords(ecs);
	}

}
