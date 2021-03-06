package com.athena.component.input;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;
/**
 * 2260 调拨明细
 * @author kong
 *
 */
public class DiaobmxDBDataWriter extends TxtWriterDBTask {
	public DiaobmxDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	} 
	
	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间
	 * */
	@Override
	public boolean beforeRecord(Record record) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		record.put("create_time",Timestamp.valueOf(df.format(new Date())));
		record.put("creator",interfaceId);
		record.put("editor",interfaceId);
		record.put("edit_time",Timestamp.valueOf(df.format(new Date())));
		try {
			record.put("shengxsj",DateTimeUtil.StringYMDToDate(record.getString("shengxsj")));
		}catch(ParseException e){
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		} 
		return true;
	}

}
