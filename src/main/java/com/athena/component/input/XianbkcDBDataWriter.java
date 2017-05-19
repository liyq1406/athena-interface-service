package com.athena.component.input;

import java.util.Date;
import java.util.List;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;

/**
 * 2630 线边库存(初始化资源表)
 * @author hzg
 *
 */
public class XianbkcDBDataWriter extends TxtWriterDBTask{
	public XianbkcDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	/**
	 * 行记录解析之后   给行记录增加修改时间
	 * */
	@Override
	public boolean beforeRecord(Record record) {	
		record.put("edit_time", new Date());
		return true;
	}

}
