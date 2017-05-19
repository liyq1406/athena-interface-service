package com.athena.component.input;

import java.util.Date;

import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 2046 EFI运输取货计划-异常申报(输入)
 * @date 2016-12-30
 * @author lc
 */
public class EFIYsqhjh_yicsbDBDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(EFIYsqhjh_yicsbDBDataReader.class);	//定义日志方法 
	public Date date = new Date();

	public EFIYsqhjh_yicsbDBDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	/**
	 * 行解析之后处理方法
	 * 给行记录增加创建人、创建时间、修改人、修改时间
	 */
	@Override
	public boolean afterRecord(Record record) {		
		record.put("create_time",date);
		record.put("creator","temp");		
		record.put("edit_time",date);
		record.put("editor","2045");
		return true;
	}
	
	/**
	 * 接口完成后处理方法  删除没有取货计划的异常申报数据
	 */
	public void after() {
		try{
			//删除没有取货计划的异常申报数据
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			        .execute("inPutzxc.deleteycsbbycreator");
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"删除没有取货计划的异常申报数据报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"删除没有取货计划的异常申报数据报错"+e.getMessage());
		}		
	}
}
