package com.athena.component.input;

import java.util.Date;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 2240 仓库循环时间
 * @author hzg
 *
 */
public class CangkxhsjDBDataWriter extends TxtInputDBSerivce {
	private  Date date = new Date();
	public CangkxhsjDBDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	/**
	 * 解析数据之前清空ckx_cangkxhsj_t表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.cangkxhsjTempDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除ckx_cangkxhsj_t表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除ckx_cangkxhsj_t表时报错"+e.getMessage());
		}
	}
	
	/**
	 * 行记录解析之后
	 */
	@Override
	public boolean afterRecord(Record record) {
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", date);
		record.put("EDITOR", interfaceId);
		record.put("EDIT_TIME", date);
		return true;
	}
	
    @Override
    public void after() {
    	//对主表标记editor为temp
		dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
		.execute("inPutzxc.cangkxhsjUpdate");
		//对主表与中间表进行合并
		dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
		.execute("inPutzxc.cangkxhsjMerge");
		 //删除主表标记editor为temp的数据
		dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
		.execute("inPutzxc.cangkxhsjDelete");
    }
	
}
