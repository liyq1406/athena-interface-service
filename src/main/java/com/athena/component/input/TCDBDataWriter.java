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
 * 1320 IN0028集装箱(TC)
 * @author hzg
 *
 */
public class TCDBDataWriter extends TxtWriterDBTask{ 

	public TCDBDataWriter(DataParserConfig dataParserConfig,
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
			record.put("QIYSJ", DateTimeUtil.StringYMDToDate(record.getString("QIYSJ")));
			record.put("YUJDDSJ", DateTimeUtil.StringYMDToDate(record.getString("YUJDDSJ")));
			record.put("DAODWLDSJ", DateTimeUtil.StringYMDToDate(record.getString("DAODWLDSJ")));
			record.put("ZUIXYJDDSJ",DateTimeUtil.StringYMDToDate(record.getString("ZUIXYJDDSJ")));
			record.put("LAXZDDDSJ", DateTimeUtil.StringYMDToDate(record.getString("LAXZDDDSJ")));
			record.put("KAIXZDSJ", DateTimeUtil.StringYMDToDate(record.getString("KAIXZDSJ")));
			record.put("GUANXSJ", DateTimeUtil.StringYMDToDate(record.getString("GUANXSJ")));
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
