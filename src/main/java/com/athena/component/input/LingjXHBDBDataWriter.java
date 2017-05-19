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
 * 1170 异常申报（零件消耗表）
 * @author Administrator
 *
 */
public class LingjXHBDBDataWriter extends TxtWriterDBTask {

	protected static Logger logger = Logger.getLogger(LingjXHBDBDataWriter.class);	//定义日志方法
	
	public LingjXHBDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	/**
	 * 行记录解析之后 
	 * hzg 2013-7-24 增加CREATOR
	 */
	@Override
	public boolean beforeRecord(Record record){
		try{
			record.put("JILRQ", DateTimeUtil.StringYMDToDate(record.getString("JILRQ")));
			record.put("CREATOR", interfaceId);
			record.put("CREATE_TIME", new Date());
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		return true;
	}

}
