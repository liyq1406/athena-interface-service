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
 * 1590 订单,2420 订单,2760订单（初始化按需）
 * @author Administrator
 *
 */
public class DingdDBDataWriter extends TxtWriterDBTask{

	public DingdDBDataWriter(DataParserConfig dataParserConfig,
			List<String> fieldList, List<String> updateFieldList,
			String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}

	/**
	 * 行解析之后处理方法
	 */
	@Override
	public boolean beforeRecord(Record record) {
		try{
			record.put("DINGDSXSJ",DateTimeUtil.StringYMDToDate(record.getString("DINGDSXSJ")));
			record.put("DINGDFSSJ",DateTimeUtil.StringYMDToDate(record.getString("DINGDFSSJ")));
			record.put("DINGDJSSJ",DateTimeUtil.StringYMDToDate(record.getString("DINGDJSSJ")));
			record.put("ZIYHQRQ",DateTimeUtil.StringYMDToDate(record.getString("ZIYHQRQ")));
		}catch(ParseException e){
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		} 
		Date date=new Date();
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", date);
		record.put("EDITOR", interfaceId);
		record.put("EDIT_TIME", date);	
		return true;
	} 

}



