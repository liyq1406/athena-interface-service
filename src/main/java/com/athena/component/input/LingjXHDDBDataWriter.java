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
 * 1740零件-消耗点,2090 零件-消耗点，3080 零件-消耗点
 * @author Administrator
 *
 */
public class LingjXHDDBDataWriter extends TxtWriterDBTask {
	public LingjXHDDBDataWriter(DataParserConfig dataParserConfig,
			List<String> fieldList, List<String> updateFieldList,
			String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}

	/**
	 * 行解析之后处理方法
	 */
	@Override
	public boolean beforeRecord(Record record) {
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", new Date());
		record.put("EDITOR", interfaceId);
		record.put("EDIT_TIME", new Date());
		try {
			record.put("SHENGXR",DateTimeUtil.StringYMDToDate(record.get("SHENGXR").toString()));
			record.put("JIESR",DateTimeUtil.StringYMDToDate(record.get("JIESR").toString()) );
		}catch(ParseException e){
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		} 
		return true;
	}
}
