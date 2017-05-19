package com.athena.component.input;

import java.util.Date;
import java.util.List;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
/**
 * 3650 整车过点
 * @author csy 2016-01-11
 *
 */
public class ZhengcgdDBDataWriter  extends TxtWriterDBTask{
	public  Date date=new Date();
	public ZhengcgdDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间
	 * */
	@Override
	public boolean beforeRecord(Record record) {	
		record.put("create_time",date);
		record.put("creator",interfaceId);
		record.put("editor",interfaceId);
		record.put("edit_time",date);
		return true;
    }
}
