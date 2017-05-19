package com.athena.component.input;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;

/**
 * 2480 DDBH拆分结果CS
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2013-12-10
 */
public class ChaifjgCSDBDataWriter extends TxtWriterDBTask {
	protected static Logger logger = Logger.getLogger(ChaifjgCSDBDataWriter.class);	
	public  Date date=new Date();
	public ChaifjgCSDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	/**
	 * 行记录解析之后   处理行记录
	 * */
	@Override
	public boolean beforeRecord(Record record) {	
		record.put("create_time",date);
		record.put("creator",interfaceId);
		record.put("editor",interfaceId);
		record.put("edit_time",date);
		try {
			record.put("emon",DateTimeUtil.StringYMDToDate(record.getString("emon")));
			record.put("xiaohsj",DateTimeUtil.StringYMDToDate(record.getString("xiaohsj")));
			record.put("caifsj",DateTimeUtil.StringYMDToDate(record.getString("caifsj")));
		}catch(ParseException e){
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		} 
		return true;
	}
	
	
	
}
