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
 * 1390  UA标签
 * @author Administrator
 *
 */
public class UABQDbDataWriter  extends TxtWriterDBTask{
	protected static Logger logger = Logger.getLogger(UABQDbDataWriter.class);	//定义日志方法

	public UABQDbDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间,并将时间类型数据格式转化成【yyyy-MM-dd HH:mm:ss】形式.
	 * */
	@Override
	public boolean beforeRecord(Record record){
		try {
			record.put("chuangjrq",DateTimeUtil.StringYMDToDate(record.getString("chuangjrq")));
			record.put("shixrq",DateTimeUtil.StringYMDToDate(record.getString("shixrq")));
			record.put("daysj",DateTimeUtil.StringYMDToDate(record.getString("daysj")));
			record.put("ruksj",DateTimeUtil.StringYMDToDate(record.getString("ruksj")));
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		Date date= new Date();
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", date);
		record.put("EDITOR", interfaceId);
		record.put("EDIT_TIME", date);
		return true;
	}
}