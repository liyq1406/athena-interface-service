package com.athena.component.input;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.date.DateUtil;

public class JtpcjhDbDateWriter extends TxtWriterDBTask {
	protected static Logger logger = Logger.getLogger(JtpcjhDbDateWriter.class);	//定义日志方法
	public JtpcjhDbDateWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
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
				record.put("jtrq",DateUtil.stringToDateYMD(DateTimeUtil.DateStr(jtrq)));
			}catch(ParseException e){
				logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
				throw new WarmBusinessException("日期转换错误！"+e.getMessage());
			}
		}
		//上线顺序号
		//将上线顺序号int型改为long型
		long t_Sxsxh = 0;
		String sxsxh = strNull(record.getString("jtxx"));
		if(StringUtils.isEmpty(sxsxh)){
			sxsxh = "0";
		}
		t_Sxsxh = Long.parseLong(sxsxh);
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
	
}
