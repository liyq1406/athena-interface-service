package com.athena.component.input;

import java.util.Date;

import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;

/**
 * 3470物流路径总图
 * @author yz
 * 2015-12-12
 *
 */
public class WlljDbDataWriter extends TxtInputDBSerivce{
	private Date date= new Date();
	private String datasourceId = "";
	protected static Logger logger = Logger.getLogger(WlljDbDataWriter.class);	//定义日志方法
	
	public WlljDbDataWriter(DataParserConfig dataParserConfig) {
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
			.execute("inPutddbh.updateckx_wullj");
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"更新editor为temp时报错"+e.getMessage());
		}
	}
	
	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间
	 * */
	@Override
	public boolean afterRecord(Record record){
		record.put("creator", interfaceId);
		record.put("create_time", date);
		record.put("editor", interfaceId);
		record.put("edit_time", date);
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
			.execute("inPutddbh.deleteckx_wullj");
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"删除editor为temp时报错"+e.getMessage());
		}
	}
	
}
