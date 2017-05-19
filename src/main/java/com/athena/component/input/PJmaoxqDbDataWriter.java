package com.athena.component.input;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.toft.utils.UUIDHexGenerator;

public class PJmaoxqDbDataWriter extends TxtWriterDBTask  {

protected static Logger logger = Logger.getLogger(PJmaoxqDbDataWriter.class);	//定义日志方法
	
	Date date = new Date();
	
	public PJmaoxqDbDataWriter(DataParserConfig dataParserConfig,
			List<String> fieldList, List<String> updateFieldList,
			String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}

	/**
	 * 行解析之后处理方法
	 * @param record 行数据集合
	 * @author hzg
	 * @update hzg 2013-5-15
	 */
	@Override
	public boolean beforeRecord(Record record) {
		record.put("xuqbc", record.getParam().get(record.getParam().get("xuqly").toString()));
		String xuqrq = record.get("xuqrq").toString();
		record.put("xuqksrq", xuqrq);
		//需求所属周期
		String xqrq=xuqrq.substring(0, 6);
		record.put("xuqsszq", xqrq);
		//产线 hzg 2013-5-16
//		String chanx =  record.getString("shiycj").concat(record.getString("chanx"));
		String chanx =  record.getString("shiycj").concat(record.getString("chanx"));
		record.put("chanx", chanx);
		try{
			//需求日期
			record.put("xuqrq",stringToDateYMD(xuqrq));
		}catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		
		//毛需求明细ID
		record.put("id", UUIDHexGenerator.getInstance().generate());
		//创建人、创建时间、修改人、修改时间
		record.put("creator", interfaceId);
		record.put("create_time", date);
		record.put("editor", interfaceId);
		record.put("edit_time", date);
		record.put("active", "1");
		String biaoz = "";
		if ("1061".equals(interfaceId)) {
			biaoz = "4";
		}else if ("1062".equals(interfaceId)) {
			biaoz = "5";
		}else if ("1063".equals(interfaceId)) {
			biaoz = "6";
		}else if ("1064".equals(interfaceId)) {
			biaoz = "7";
		}
		record.put("biaos", biaoz);
		return true;
	}
	
	
	/**
	 * 将字符串转换为yyyyMMdd日期类型
	 * @date 2013-4-15
	 * @author hzg
	 * @param str 日期字符串
	 * @return Date 日期
	 */
	public Date stringToDateYMD(String str) throws ParseException{
		DateFormat yyyyMMddFormat = new SimpleDateFormat("yyyyMMdd",Locale.CHINA);
		Date date = null; 
		if (StringUtils.isNotEmpty(str)) {
			date = yyyyMMddFormat.parse(str); 
		}
		return date;
	}
	
	
}
