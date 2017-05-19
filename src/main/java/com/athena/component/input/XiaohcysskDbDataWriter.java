package com.athena.component.input;

import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.exception.ServiceException;
import com.toft.core3.transaction.annotation.Transactional;

/**
 * 2390 小火车运输时刻表
 * @author hzg
 *
 */
public class XiaohcysskDbDataWriter extends TxtInputDBSerivce{
	
	private Date date= new Date();
	
	public XiaohcysskDbDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	
	/**
	 * 解析数据之前清空ckx_xiaohcyssk表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.xiaohcysskTempDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除ckx_xiaohcyssk表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除ckx_xiaohcyssk表时报错"+e.getMessage());
		}
	}
	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间,并将时间类型数据格式转化成【yyyy-MM-dd HH:mm:ss】形式.
	 * */
	@Override
	public boolean afterRecord(Record record) {
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", date);
		record.put("EDITOR", interfaceId);
		record.put("EDIT_TIME", date);
		try {
			record.put("kaisbhsj",StringUtils.isEmpty(record.getString("kaisbhsj").trim())?"":DateTimeUtil.StringYMDToDate(record.getString("kaisbhsj")));
			record.put("chufsxsj",StringUtils.isEmpty(record.getString("chufsxsj").trim())?"":DateTimeUtil.StringYMDToDate(record.getString("chufsxsj")));
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		return true;
	}
	
	/**
	 * Merge表ckx_xiaohcyssk表数据
	 */
	@Override
	@Transactional
	public void after() {
		try{
			logger.info("线程--接口" + interfaceId +"Merge表ckx_xiaohcyssk开始");
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.xiaohcysskUpdate");

			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.xiaohcysskMerge");
			
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.xiaohcysskDelete");
			logger.info("线程--接口" + interfaceId +"Merge表ckx_xiaohcyssk结束");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"Merge表ckx_xiaohcyssk表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除ckx_xiaohcyssk表时报错"+e.getMessage());
		}
	}
}
