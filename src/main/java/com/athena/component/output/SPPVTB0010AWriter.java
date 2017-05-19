package com.athena.component.output;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;

public class SPPVTB0010AWriter extends TxtInputDBSerivce {

	public SPPVTB0010AWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}

	@Override
	public void before() {
		dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
		.execute("inPutzxc.deleteSppvTba");
		
		dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
		.execute("inPutzxc.deleteSppvTbb");
		
		dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
		.execute("inPutzxc.deleteSppvTbGdmxq");
	}
	
	
	

}
