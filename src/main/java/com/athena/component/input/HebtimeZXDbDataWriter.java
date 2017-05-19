package com.athena.component.input;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;

/**
 * 2860,2870,3210,3220 合并时间-详细，工作时间模板共用此类
 * 生成创建人和创建日期
 * @author hzg
 *
 */
public class HebtimeZXDbDataWriter extends TxtWriterDBTask{
	
	protected static Logger logger = Logger.getLogger(HebtimeZXDbDataWriter.class);	//定义日志方法
	
	private Date date= new Date();
	
	public HebtimeZXDbDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间,并将时间类型数据格式转化成【yyyy-MM-dd HH:mm:ss】形式.
	 * */
	@Override
	public boolean beforeRecord(Record record) {
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", date);
		return true;
	}
}
