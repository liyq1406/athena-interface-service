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
 * 2220 运输时刻
 * @author hzg
 *
 */
public class YunsskDBDataWriter extends TxtInputDBSerivce {

	private  Date date = new Date();
	public YunsskDBDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	/**
	 * 解析数据之前清空ckx_yunssk_t表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.yunsskTempDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除ckx_yunssk_t表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除ckx_yunssk_t表时报错"+e.getMessage());
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
			record.put("FACSJ",DateTimeUtil.StringYMDToDate(record.get("FACSJ").toString()) );
			record.put("DAOHSJ",DateTimeUtil.StringYMDToDate(record.get("DAOHSJ").toString()));
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		return true;
	}

    @Override
    public void after() {
    	//对主表标记editor为temp
		dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
		.execute("inPutzxc.yunsskUpdate");
		//对主表与中间表进行合并
		dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
		.execute("inPutzxc.yunsskMerge");
		 //删除主表标记editor为temp的数据
		dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
		.execute("inPutzxc.yunsskDelete");
    }
}
