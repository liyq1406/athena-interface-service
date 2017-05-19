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
 *2430 ,2480 DDBH拆分结果
 * @author kong
 * @update hzg
 */
public class ChaifjgHDBDataWriter extends TxtWriterDBTask{
	public  Date date=new Date();
	public ChaifjgHDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	
	
	/**
	 * 行记录解析之后   处理行记录
	 * */
	@Override
	public boolean beforeRecord(Record record) {	
		/*record.put("flag", "1");
		record.put("create_time",date);
		record.put("creator",interfaceId);
		record.put("editor",interfaceId);
		record.put("edit_time",date);*/
		try {
			record.put("emon",DateTimeUtil.StringYMDToDate(record.getString("emon")));
			//此类 2430  调用，由于列名不统一，所以需要分别解析
			//record.put("emonsj",DateTimeUtil.StringYMDToDate(record.getString("emonsj")));
			//record.put("xhsj",DateTimeUtil.StringYMDToDate(record.getString("xhsj")));
			record.put("xiaohsj",DateTimeUtil.StringYMDToDate(record.getString("xiaohsj")));
			record.put("caifsj",DateTimeUtil.StringYMDToDate(record.getString("caifsj")));
			//record.put("chaifsj",DateTimeUtil.StringYMDToDate(record.getString("chaifsj")));
		}catch(ParseException e){
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		} 
		return true;
	}
}
