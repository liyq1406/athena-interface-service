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

/**
 * 1400 外部要货令
 * @author Administrator
 *
 */
public class YaohlDbDataWriter extends TxtWriterDBTask{

	protected static Logger logger = Logger.getLogger(YaohlDbDataWriter.class);	//定义日志方法


	public YaohlDbDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}

	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间,并将时间类型数据格式转化成【yyyy-MM-dd HH:mm:ss】形式.
	 * */
	@Override
	public boolean beforeRecord(Record record){
		try {
			record.put("jiaofj",StringUtils.isEmpty(record.getString("jiaofj").trim())?"":DateTimeUtil.StringYMDToDate(record.getString("jiaofj")));
			record.put("zuizsj",StringUtils.isEmpty(record.getString("zuizsj").trim())?"":DateTimeUtil.StringYMDToDate(record.getString("zuizsj")));
			record.put("zuiwsj",StringUtils.isEmpty(record.getString("zuiwsj").trim())?"":DateTimeUtil.StringYMDToDate(record.getString("zuiwsj")));
			record.put("faysj",StringUtils.isEmpty(record.getString("faysj").trim())?"":DateTimeUtil.StringYMDToDate(record.getString("faysj")));
			record.put("shangxsj",StringUtils.isEmpty(record.getString("shangxsj").trim())?"":DateTimeUtil.StringYMDToDate(record.getString("shangxsj")));
			record.put("yaohlscsj",StringUtils.isEmpty(record.getString("yaohlscsj").trim())?"":DateTimeUtil.StringYMDToDate(record.getString("yaohlscsj")));
			record.put("xiughyjjfsj",StringUtils.isEmpty(record.getString("xiughyjjfsj").trim())?"":DateTimeUtil.StringYMDToDate(record.getString("xiughyjjfsj")));
			record.put("shijfysj",StringUtils.isEmpty(record.getString("shijfysj").trim())?"":DateTimeUtil.StringYMDToDate(record.getString("shijfysj")));
			record.put("laxzdddsj",StringUtils.isEmpty(record.getString("laxzdddsj").trim())?"":DateTimeUtil.StringYMDToDate(record.getString("laxzdddsj")));
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}

		Date date= new Date();
		record.put("creator", interfaceId);
		record.put("create_time", date);
		record.put("editor", interfaceId);
		record.put("edit_time", date);
		return true;
	}
}
