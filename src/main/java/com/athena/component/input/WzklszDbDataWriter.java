package com.athena.component.input;


import java.text.ParseException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.date.DateUtil;
import com.toft.utils.UUIDHexGenerator;

/**
 * 1420,2720 外租库流水账
 * @author Administrator
 *
 */
public class WzklszDbDataWriter extends TxtWriterDBTask{

	protected static Logger logger = Logger.getLogger(WzklszDbDataWriter.class);	//定义日志方法

	public WzklszDbDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
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
		String lingjsl1 = sb.delete(0,1).toString();
		double lingjsl2 = Double.parseDouble(StringUtils.isEmpty(lingjsl1)?"0":lingjsl1);
		record.put("lingjsl", lingjsl2/1000);
		try {
			record.put("jfrq",DateUtil.stringToDateYMD(DateUtil.StringFormatWithLine(record.getString("jfrq"))));
			record.put("rksj",DateUtil.stringToDateYMDHM(DateUtil.StringFormatWithLineYMDHM(record.getString("rksj"))));
			record.put("dqsj",DateUtil.stringToDateYMD(DateUtil.StringFormatWithLine(record.getString("dqsj"))));

		} catch (ParseException e) {//日期出错 hzg 2013-3-1
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		return true;

	}
}
