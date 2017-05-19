package com.athena.component.input;

import java.text.ParseException;
import java.util.Date;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.exception.ServiceException;
/**
 * 1150 DDBH拆分结果sppv
 * @author kong  hzg
 *
 */
public class ChaifjgSppvDBDataWriter extends TxtInputDBSerivce{
	public ChaifjgSppvDBDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}

	
	/**
	 * 数据解析之前清空零件xqjs_anxmaoxq表数据，根据xhsj增量删除
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzbc.anxmaoxqDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除xqjs_anxmaoxq表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除xqjs_anxmaoxq表时报错"+e.getMessage());
		}
	}
	
	/**
	 * 行解析之后处理
	 * record 行结果集
	 */
	@Override
	public boolean afterRecord(Record record){
		try {
			record.put("emon",DateTimeUtil.StringYMDToDate(record.getString("emon")));
			//此类分别给1150    2430  调用，由于列名不统一，所以需要分别解析
			record.put("emonsj",DateTimeUtil.StringYMDToDate(record.getString("emonsj")));
			record.put("xhsj",DateTimeUtil.StringYMDToDate(record.getString("xhsj")));
			record.put("xiaohsj",DateTimeUtil.StringYMDToDate(record.getString("xiaohsj")));
			record.put("caifsj",DateTimeUtil.StringYMDToDate(record.getString("caifsj")));
			record.put("chaifsj",DateTimeUtil.StringYMDToDate(record.getString("chaifsj")));
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		Date date=new Date();
		record.put("flag", "0");
		record.put("create_time",date);
		record.put("creator",interfaceId);
		record.put("editor",interfaceId);
		record.put("edit_time",date);
		return true;
	}

}
