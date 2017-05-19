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
 * 2190 订单明细,2770 订单明细（初始化按需）
 * @author hzg
 *
 */
public class DingdmxDBDataWriter extends TxtWriterDBTask{

	private Date date=new Date();
	public DingdmxDBDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}

	/**
	 * 行解析之后处理方法
	 * @param record 行数据集合
	 * @author GJ
	 */
	@Override
	public boolean beforeRecord(Record record) {
		try{
			record.put("YAOHQSRQ",DateTimeUtil.StringYMDToDate(record.getString("YAOHQSRQ")));
			record.put("YAOHJSRQ",DateTimeUtil.StringYMDToDate(record.getString("YAOHJSRQ")));
			record.put("FAYRQ", DateTimeUtil.StringYMDToDate(record.getString("FAYRQ")));
			record.put("JIAOFRQ",DateTimeUtil.StringYMDToDate(record.getString("JIAOFRQ")));
			record.put("ZUIHWHSJ",DateTimeUtil.StringYMDToDate(record.getString("ZUIHWHSJ")));
			record.put("SHID",DateTimeUtil.StringYMDToDate(record.getString("SHID")));
			record.put("ZUIZDHSJ",DateTimeUtil.StringYMDToDate(record.getString("ZUIZDHSJ")));
			record.put("ZUIWDHSJ",DateTimeUtil.StringYMDToDate(record.getString("ZUIWDHSJ")));
			record.put("XIAOHSJ",DateTimeUtil.StringYMDToDate(record.getString("XIAOHSJ")));
			record.put("PCKAISSJ",DateTimeUtil.StringYMDToDate(record.getString("PCKAISSJ")));
			record.put("PCJIESSJ",DateTimeUtil.StringYMDToDate(record.getString("PCJIESSJ")));
			record.put("PCEDIT_TIME",DateTimeUtil.StringYMDToDate(record.getString("PCEDIT_TIME")));
			record.put("GONGZR",DateTimeUtil.StringYMDToDate(record.getString("GONGZR")));
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", date);
		record.put("EDITOR", interfaceId);
		record.put("EDIT_TIME", date);
		return true;
	}
	
	
	

}
