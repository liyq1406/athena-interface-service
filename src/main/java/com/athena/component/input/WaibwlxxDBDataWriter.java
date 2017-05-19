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
 * 2750 外部物流路径详细
 * @author hzg
 *
 */
public class WaibwlxxDBDataWriter  extends TxtInputDBSerivce{
	public Date date=new Date();
	public WaibwlxxDBDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}

	/**
	 * 解析数据之前清空ckx_waibwlxx中间表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.waibwlxxDelete_temp");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除ckx_waibwlxx_t表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除ckx_waibwlxx_t表时报错"+e.getMessage());
		}
	}
	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间
	 * */
	@Override
	public boolean afterRecord(Record record){
		record.put("create_time",date);
		record.put("creator",interfaceId);
		record.put("editor",interfaceId);
		record.put("edit_time",date);
		try{
			record.put("shengxrq",DateTimeUtil.StringYMDToDate(record.getString("shengxrq")));
		}catch(ParseException e){
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		} 
		return true;
	}

    @Override
    public void after() {
        //更新ckx_waibwlxx的editor为temp
        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
                .execute("inPutzxc.updateWaibwlxxTemp");
        //中间表与主表合并
        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
                .execute("inPutzxc.mergeWaibwlxx");
        //删除ckx_waibwlxx的editor为temp
        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
                .execute("inPutzxc.deleteWaibwlxxTemp");
    }
}
