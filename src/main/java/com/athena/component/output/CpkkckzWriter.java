package com.athena.component.output;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;

/**
 * 2640 成品库库存快照 
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2013-8-20
 */
public class CpkkckzWriter extends TxtInputDBSerivce {

	public CpkkckzWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}

	/**
	 * 解析前清空成品库库存快照表
	 */
	@Override
	public void before() {
		dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
		.execute("outPut.deleteCpkkckz");
	}
	
	
	

}
