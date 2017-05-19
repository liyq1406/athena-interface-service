package com.athena.component.output;

import java.util.HashMap;
import java.util.Map;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;

public class SPPVTB0010Writer extends DBOutputTxtSerivce {

	

	public SPPVTB0010Writer(DataParserConfig dataParserConfig) {

	}


	public void before() {
		Map<String,String> param = new HashMap<String,String>();
		baseDao.getSdcDataSource(sourceId).execute("outPut.insertSPPVTB",param);
	}
	
	
	

}
