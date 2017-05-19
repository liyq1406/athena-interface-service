package com.athena.component.input;

import java.util.Date;


import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;

/**
 * 3390
 * 解析后增加创建人、创建时间、修改人、修改时间
 * 清除多余数据
 * @author yz 2016-1-12
 *
 */
public class Ckx_fenzxpcslDBDataWriter extends TxtInputDBSerivce{
	public  Date date=new Date();
	private String datasourceId = "";
	
	public Ckx_fenzxpcslDBDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}

	
	/**
	 * 接口运行前处理方法
	 * 将editor改为temp
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.updateckx_fenzxpcsl");
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"更新editor为temp时报错"+e.getMessage());
		}
	}



	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间
	 **/
	@Override
	public boolean afterRecord(Record record) {	
		record.put("create_time",date);
		record.put("creator",interfaceId);
		record.put("editor",interfaceId);
		record.put("edit_time",date);
		return true;
	}
	
	/**
	 * 接口运行完处理方法
	 * 将editor为temp的删除
	 */
	@Override
	public void after() {		
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.deleteckx_fenzxpcsl");
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"删除editor为temp时报错"+e.getMessage());
		}
	}
	
}
