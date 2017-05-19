package com.athena.component.input;

import java.text.ParseException;

import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;

/**
 * 1160 资源快照
 * @author hzg
 *
 */
public class KuckzDbDataWriter extends TxtInputDBSerivce{
	
	protected static Logger logger = Logger.getLogger(KuckzDbDataWriter.class);	//定义日志方法
	
	public KuckzDbDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig); 
	}
	

	/**
	 * 数据解析之前清空零件xqjs_ziykzb表数据
	 * 带上用户中心  hzg 2014.3.22
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzbc.ziykzbDelete",dataParserConfig.getUsercenter());
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除xqjs_ziykzb表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除xqjs_ziykzb表时报错"+e.getMessage());
		}
	}
	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间,并将时间类型数据格式转化成【yyyy-MM-dd HH:mm:ss】形式.
	 * 资源快照表无creator、create_time等字段
	 * */
	@Override
	public boolean afterRecord(Record record){
		try {
			record.put("ziyhqrq",DateUtil.stringToDateYMD(record.getString("ziyhqrq")));
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		return true;
	}
}
