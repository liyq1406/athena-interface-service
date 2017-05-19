package com.athena.component.input;

import java.util.Date;
import java.util.List;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
/**
 * 解析后增加创建人、创建时间、修改人、修改时间
 * @author csy
 *
 */
public class AnxjlxqDBDataWriter extends TxtWriterDBTask{
	public  Date date=new Date();
	public AnxjlxqDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}

	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间
	 * */
	@Override
	public boolean beforeRecord(Record record) {	
		record.put("CREATE_TIME",date);
		record.put("CREATOR",interfaceId);
		record.put("EDITOR",interfaceId);
		record.put("EDIT_TIME",date);
		return true;
	}
}
