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
 * 2110 生产线,3100 生产线
 * @author hzg
 *
 */
public class ShengcxDBDataWriter extends TxtWriterDBTask {

	private  Date date = new Date();
	public ShengcxDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	/**
	 * 行解析之后处理方法
	 * @param record 行数据集合
	 */
	@Override
	public boolean beforeRecord(Record record) {	
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", date);
		record.put("EDITOR", interfaceId);
		record.put("EDIT_TIME", date);
		try {
			record.put("QIEHSJ",DateTimeUtil.StringYMDToDate(record.get("QIEHSJ").toString()) );
		}catch(ParseException e){
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		} 
		return true;
	}

}
