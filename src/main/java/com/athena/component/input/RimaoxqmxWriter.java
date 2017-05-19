package com.athena.component.input;

import java.util.Date;
import java.util.List;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.toft.utils.UUIDHexGenerator;

/**
 * 3130 生产平台
 * @author hzg
 *
 */
public class RimaoxqmxWriter extends TxtWriterDBTask{
	private Date date= new Date();
	public RimaoxqmxWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间
	 * */
	@Override
	public boolean beforeRecord(Record record) {
		record.put("id", UUIDHexGenerator.getInstance().generate());
		return true;
	}
}
