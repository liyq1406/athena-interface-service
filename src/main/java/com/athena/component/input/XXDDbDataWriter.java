package com.athena.component.input;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;

/**
 * 1080 消耗点参考系
 * @author hzg
 *
 */
public class XXDDbDataWriter extends TxtWriterDBTask {
	protected static Logger logger = Logger.getLogger(XXDDbDataWriter.class);	//定义日志方法
	
	public XXDDbDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}

	
	/**
	 * 行解析之后处理方法
	 * @param record 行数据集合
	 */
	@Override
	public boolean beforeRecord(Record record){
		record.put("CREATOR", interfaceId) ;
		record.put("EDITOR", interfaceId) ;
		record.put("GONGYBS", "1") ;
		record.put("BIAOS", "2") ;
		record.put("CREATE_TIME", new Date()) ;
		record.put("EDIT_TIME", new Date()) ;
		return true;
	}
}
