package com.athena.component.input;

import java.util.Date;

import org.apache.log4j.Logger;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 2010 外部订单预告
 * @author hzg
 *
 */
public class WbddygDbDataWriter extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(WbddygDbDataWriter.class);	//定义日志方法
	public WbddygDbDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig); 
	}

	
	/**
	 * 解析数据之前清空in_wbddyg表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.wbddygDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除in_wbddyg表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除in_wbddyg表时报错"+e.getMessage());
		}
	}
	
	/**
	 * 解析后操作
	 */
	@Override
	public boolean afterRecord(Record record){
		String shul=record.get("shul").toString();
		int i_shul=Integer.parseInt(shul);
		record.put("shul", i_shul);
		
		//存入创建时间和处理状态初始值
		record.put("cj_date", new Date());
		record.put("clzt", 0);
		return true;
	}
}
