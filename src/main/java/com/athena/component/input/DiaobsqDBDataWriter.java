package com.athena.component.input;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;
/**
 * 2250 调拨申请
 * @author kong
 *
 */
public class DiaobsqDBDataWriter extends TxtWriterDBTask{ 
	public DiaobsqDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间
	 * */
	@Override
	public boolean beforeRecord(Record record) {
		record.put("create_time",new Date());
		record.put("creator",interfaceId);
		record.put("editor",interfaceId);
		record.put("edit_time",new Date());
		try{
			record.put("diaobsqsj",DateTimeUtil.StringYMDToDate(record.getString("diaobsqsj")));
		}catch(ParseException e){
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		} 
		return true;
	}

}
