package com.athena.component.input;


import java.text.ParseException;
import java.util.List;

import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.toft.utils.UUIDHexGenerator;

public class CKWzklszDbDataWriter extends TxtWriterDBTask{
	
	protected static Logger logger = Logger.getLogger(CKWzklszDbDataWriter.class);	//定义日志方法

	public CKWzklszDbDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	/**
	 * 行记录解析之后   将时间类型数据格式转化成相应的时间格式形式.
	 * */
	@Override
	public boolean beforeRecord(Record record){
		record.put("ID", UUIDHexGenerator.getInstance().generate());
		String lingjsl=record.getString("lingjsl");
		StringBuilder sb=new StringBuilder(lingjsl);
		String lingjsl1=sb.delete(0,1).toString();
		double lingjsl2=Double.parseDouble(lingjsl1);
		record.put("lingjsl", lingjsl2/1000);
		try {	
			record.put("jfrq",DateTimeUtil.StringYMDToDate(DateTimeUtil.DateFormat(record.getString("jfrq"))));
			record.put("rksj",DateTimeUtil.StringYMDToDate(DateTimeUtil.DateFormat_Fhtz(record.getString("rksj")) ));
			record.put("dqsj",DateTimeUtil.StringYMDToDate(DateTimeUtil.DateFormat(record.getString("dqsj"))));

		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		return true;
	}
}
