package com.athena.component.output;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;

/**
 * 1250 零件供应商
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2014-2-27
 */
public class LingjgysDataWriter extends DBOutputTxtSerivce {
	public LingjgysDataWriter(DataParserConfig dataParserConfig) {
			
	}
	
	/**
	 * 1250输出完成后，将Flag状态1改为0
	 */
	@Override
	public void after(){
		baseDao.getSdcDataSource(sourceId).execute("outPut.updateFlagOfLingjgys");
	}
	
}
