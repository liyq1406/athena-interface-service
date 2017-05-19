package com.athena.component.input;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 2980 零件库存快照(输入)
 * @date 2015-12-31
 * @author lc
 */
public class In_dfpv_kuckzDbDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(In_dfpv_kuckzDbDataReader.class);	//定义日志方法 
	public Date date = new Date();

	public In_dfpv_kuckzDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	/**
	 * 数据解析之前保留最近六版数据，其他清除，并删除in_dfpv_kuckz表当天数据
	 */
	@Override
	public void before() {
		try{
			logger.info("接口" + interfaceId +" 保留最近六版数据，其他清除，并删除in_dfpv_kuckz表当天数据开始");
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.dfpvkckzDelete");
			logger.info("接口" + interfaceId +" 保留最近六版数据，其他清除，并删除in_dfpv_kuckz表当天数据结束");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除in_dfpv_kuckz表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除in_dfpv_kuckz表时报错"+e.getMessage());
		}
	}
	
	/**
	 * 行解析之后处理方法
	 * 给行记录增加创建时间、修改时间
	 * 将零件编号不存在和零件编号的值超过定义长度的行数据过滤掉
	 */
	@Override
	public boolean afterRecord(Record record) {		
		boolean result = true;
		String lingjbh = record.getString("LINGJBH").trim();
		String fuh = record.getString("FUH").trim();
		String lingjsl = record.getString("LINGJSL").trim();
		if(StringUtils.isEmpty(lingjbh)||lingjbh.length()>10){
			result = false;
		}				
		if("-".equals(fuh)){
			record.put("LINGJSL",fuh+lingjsl);
		}
		record.put("EDIT_TIME",date);
		record.put("CREATE_TIME",date);		
		return result;
	}
	
	/**
	 * 接口完成后处理方法 查询得到单位换算后的单位和数量
	 */
	@SuppressWarnings("unchecked")
	public void after() {
		// 查询得到单位换算后的单位和数量
		List<Map<String, Object>> lingjdwList = dataParserConfig.getBaseDao()
				.getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId()).select("inPutzxc.queryshuldanw");
		// 更新零件单位和数量
		for (Map<String, Object> map : lingjdwList) {
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
					.execute("inPutzxc.updateshuldanw", map);
		}
	}
}
