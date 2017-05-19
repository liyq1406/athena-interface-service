package com.athena.component.input;

import java.util.Date;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 2200物流路径总图
 * @author hzg
 * 
 *
 */
public class WulljDbDataWriter extends TxtInputDBSerivce{
	private Date date= new Date();
	private String datasourceId = "";
	
	public WulljDbDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	
	
	/**
	 * 解析数据之前清空ckx_wullj表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutzxc.wulljDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除ckx_wullj表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除ckx_wullj表时报错"+e.getMessage());
		}
	}
	
	
	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间
	 * */
	@Override
	public boolean afterRecord(Record record){
		record.put("creator", interfaceId);
		record.put("create_time", date);
		record.put("editor", interfaceId);
		record.put("edit_time", date);
		return true;
	}
	
	@Override
	public void after() {
	    	//对主表标记editor为temp
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutzxc.wulljUpdateTemp");
			//对主表与中间表进行合并
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutzxc.wulljMerge");
			 //删除主表标记editor为temp的数据
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutzxc.wulljDeleteTemp");
					
			 //将ck_csinfo表中当前时间大于等于‘将来模式生效时间’的数据转入ck_csinfo_h表中 	
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutzxc.insertWulljCsinfo");
			
		    //删除ck_csinfo表中当前时间大于等于‘将来模式生效时间’的数据
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutzxc.deleteWulljCsinfo");
			
			//筛选ckx_wullj表中存在，但ck_csinfo表中不存在的数据，将中符合条件转入ck_csinfo表中
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutzxc.insertWulljLCsinfo");	
			
			//筛选ckx_wullj表中不存在，但ck_csinfo表中存在的数据，将中符合条件转入ck_csinfo_h表中		
             dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.insertWulljRCsinfo");		
     
             //删除ck_csinfo表中ckx_wullj表中不存在，但ck_csinfo表存在的数据的数据
		    dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.deletefCsinfo");		
			
	    }
}
