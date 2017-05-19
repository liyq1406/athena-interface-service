package com.athena.component.input;

import java.util.Date;
import java.util.List;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
/**
 * 1930 DDBH拆分异常报警
 * @author kong
 *
 */
public class DDBHYccfDBDataWriter extends TxtWriterDBTask{
	
	public DDBHYccfDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	
	/**
	 * 行解析之后处理
	 * record 行结果集
	 */
	@Override
	public boolean beforeRecord(Record record){
		record.put("CREATOR", interfaceId) ;
		record.put("CREATE_TIME", new Date()) ;
		record.put("EDITOR", interfaceId) ;
		record.put("EDIT_TIME", new Date()) ;
		return true;
	}

}
