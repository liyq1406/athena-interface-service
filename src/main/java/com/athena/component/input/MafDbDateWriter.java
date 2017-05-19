package com.athena.component.input;

import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 1010 零件MAF库存接口输入类
 * @author HZG
 * @date 2013-1-30
 *
 */
public class MafDbDateWriter extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(MafDbDateWriter.class);	//定义日志方法
	protected String dataSourceId = "";
	public MafDbDateWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		dataSourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	
	/**
	 * 数据解析之前清空零件MAF库存表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataSourceId)
			.execute("inPutzbc.mafDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除in_maf表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除in_maf表时报错"+e.getMessage());
		}
	}
	
	
	
	/**
	 * 行解析之后处理方法
	 * @param record 行数据集合
	 * @author GJ
	 */
	@Override
	public boolean afterRecord(Record record) {
			String ck_date=record.getString("ck_date");//获得库存时间
			record.put("ck_date", DateTimeUtil.DateStr(ck_date));//时间格式化

			//库存数量格式转换
			String num=record.getString("n_maf_num").trim();
			if(StringUtils.isNotEmpty(num)){
				record.put("n_maf_num", num);
			}

			//卷号取空格
			String juanh=record.getString("juanh").trim();
			if(StringUtils.isNotEmpty(juanh)){
				record.put("juanh", juanh);
			}
			//存入创建时间和处理状态初始值
			record.put("cj_date", new Date());
		return true;
	}
}
