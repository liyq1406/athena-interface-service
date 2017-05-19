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
 * 1380 到货通知单
 * @author Administrator
 *
 */
public class DaohtzdDbDataWriter extends TxtInputDBSerivce{ 
	public DaohtzdDbDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}

	/**
	 * 数据解析之前清空零件ck_daohtzd表数据
	 */
	@Override
	public void before() {
		try{
			//logger.info("用户中心-------->"+dataParserConfig.getUsercenter());
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzbc.daohtzdDelete",dataParserConfig.getUsercenter());
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除ck_daohtzd表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除ck_daohtzd表时报错"+e.getMessage());
		}
	}


	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间,并将时间类型数据格式转化成【yyyy-MM-dd HH:mm:ss】形式.
	 * @param record 行结果集
	 */
	@Override
	public boolean afterRecord(Record record) {
		try {
			record.put("utscsj",DateTimeUtil.StringYMDToDate(record.getString("utscsj")));
			record.put("yujddsj",DateTimeUtil.StringYMDToDate(record.getString("yujddsj")));
			record.put("blscsj",DateTimeUtil.StringYMDToDate(record.getString("blscsj")));
			record.put("yanssj",DateTimeUtil.StringYMDToDate(record.getString("yanssj")));
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", new Date());
		record.put("EDITOR", interfaceId);
		record.put("EDIT_TIME", new Date());
		return true;
	}
}
