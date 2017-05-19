package com.athena.component.input;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;

/**
 * 2820模式切换记录,2400 生产线-线边仓库
 * @author hzg
 *
 */
public class ShengcxXianbDbDataWriter extends TxtWriterDBTask{
	protected static Logger logger = Logger.getLogger(ShengcxXianbDbDataWriter.class);	//定义日志方法
	
	private Date date= new Date();
	
	public ShengcxXianbDbDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间
	 * */
	@Override
	public boolean beforeRecord(Record record) {
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", date);
		record.put("EDITOR", interfaceId);
		record.put("EDIT_TIME", date);
		return true;
	}
}
