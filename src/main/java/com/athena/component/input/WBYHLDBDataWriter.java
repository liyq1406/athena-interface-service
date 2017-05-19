package com.athena.component.input;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;

public class WBYHLDBDataWriter extends TxtWriterDBTask{
	public WBYHLDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}

	/**
	 * 行记录解析之后
	 * @return
	 */
	@Override
	public boolean beforeRecord(Record record){
		try{
		record.put("JIAOFJ",DateTimeUtil.StringYMDToDate(record.getString("JIAOFJ")));
		record.put("ZUIZSJ",DateTimeUtil.StringYMDToDate(record.getString("ZUIZSJ")));
		record.put("ZUIWSJ",DateTimeUtil.StringYMDToDate(record.getString("ZUIWSJ")));
		record.put("FAYSJ",DateTimeUtil.StringYMDToDate(record.getString("FAYSJ")));
		//record.put("BEIHSJ",DateTimeUtil.StringYMDToDate(record.getString("BEIHSJ")));
		record.put("SHANGXSJ",DateTimeUtil.StringYMDToDate(record.getString("SHANGXSJ")));
		record.put("YAOHLSCSJ",DateTimeUtil.StringYMDToDate(record.getString("YAOHLSCSJ")));
		record.put("SHIJFYSJ",DateTimeUtil.StringYMDToDate(record.getString("SHIJFYSJ")));
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		Date date=new Date();
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", date);
		record.put("EDITOR", "2560temp");
		record.put("EDIT_TIME", date);
		return true;
	}
	
}
