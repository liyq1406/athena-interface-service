package com.athena.component.input;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.date.DateUtil;

/**
 * 3130 生产平台
 * @author hzg
 *
 */
public class ShengcptDBDataWriter extends TxtWriterDBTask{
	private Date date= new Date();
	public ShengcptDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间
	 * */
	@Override
	public boolean beforeRecord(Record record) {
		record.put("creator", "interface");
		record.put("create_time", date);
		record.put("editor", "interface");
		record.put("edit_time", date);
		try {
			record.put("qiehsj", DateUtil.stringToDateYMDHMS(record.getString("qiehsj")));
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		return true;
	}
}
