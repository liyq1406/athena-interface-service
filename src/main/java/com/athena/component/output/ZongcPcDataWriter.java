package com.athena.component.output;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.exception.ServiceException;
/**
 * 襄阳总成排产
 * @author xss
 * @vesion 1.0
 * @date 2016-1-18
 * 
 */

public class ZongcPcDataWriter extends DBOutputTxtSerivce {
	public ZongcPcDataWriter(DataParserConfig dataParserConfig) {
		
	}
	
	@Override
	public void before(){
		/** 清除18个月前数据*/
		int num = dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.deleteZCPC");
		
		logger.info("清除18个月前数据共"+num+"条");	}
	
	
}
