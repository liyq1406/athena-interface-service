package com.athena.component.input;

import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;

/**
 * 2710 外租库存接口输入类
 * @author PR
 *
 */
public class WzkcDbDateWriter extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(WzkcDbDateWriter.class);	//定义日志方法
	public WzkcDbDateWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	/**
	 * 数据解析之前清空零件MAF库存表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.wzkcDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除外租库存表数据时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除外租库存表数据时报错"+e.getMessage());
		}
	}

	/**
	 * 行解析之后处理方法
	 * @param rowIndex 行标
	 * @param record 行数据集合
	 * @author PR
	 */
	@Override
	public boolean afterRecord(Record record) {
		try{
			String ck_date=record.getString("ck_date");//获得库存时间
			record.put("ck_date", DateUtil.StringFormatWithLine(ck_date));//时间格式化
		}catch(Exception e){
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
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
		//record.put("clzt", 0);
		return true;
	}
}
