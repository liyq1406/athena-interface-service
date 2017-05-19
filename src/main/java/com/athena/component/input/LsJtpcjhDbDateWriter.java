package com.athena.component.input;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.date.DateUtil;

public class LsJtpcjhDbDateWriter extends TxtWriterDBTask {
	protected static Logger logger = Logger.getLogger(LsJtpcjhDbDateWriter.class);	//定义日志方法
	public LsJtpcjhDbDateWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
    /**
     * 解析后操作
     */
	@Override
	public boolean beforeRecord(Record record) {
		String jtrq=record.getString("jtrq");
		if(StringUtils.isNotEmpty(jtrq)){
			try{ 
				record.put("jtrq",DateUtil.stringToDateYMD(DateStr(jtrq))); 
			}catch(ParseException e){
				logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
				throw new WarmBusinessException("日期转换错误！"+e.getMessage());
			}
		}
		//上线顺序号
		int t_Sxsxh = 0;
		String sxsxh = strNull(record.getString("jtxx"));
		if(StringUtils.isEmpty(sxsxh)){
			sxsxh = "0";
		}
		t_Sxsxh = Integer.parseInt(sxsxh);
		record.put("jtxx",t_Sxsxh);
		//存入创建时间和处理状态初始值
		record.put("cj_date", new Date());
		record.put("clzt", 0);
		return true;
	}
	
	/**
	 * 空串处理
	 * @param obj 对象
	 * @return 处理后字符串
	 * @date 2011-10-26
	 */
	private String strNull(Object obj) {// 对象为空返回空串,不为空toString
		return obj == null ? "" : obj.toString().trim();
	}
	
	/**
	 * 日期格式转换(00.00.0000)
	 * @param date
	 * @return
	 */
	public String DateStr(String date) {
		String dateTime = "";
		try {
			if (null != date && !"".equals(date)) {
				String dd = date.substring(0,2);
				String mm =  date.substring(3,5);
				String yyyy =  date.substring(6,10);
				dateTime = yyyy + "-" + mm + "-" + dd;
			}
		} catch (RuntimeException e) {
			throw new RuntimeException(e.getMessage());
		}
		return dateTime;
	}
}
