package com.athena.component.input;

import java.util.Date;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;
/**
 * 2230 外部物流路径
 * @author kong hzg
 *
 */
public class WaibwlDBDataWriter extends TxtInputDBSerivce{
	public static Date date=new Date();
	public WaibwlDBDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	/**
	 * 解析数据之前清空ckx_waibwl 中间表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzxc.waibwlDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除ckx_waibwl表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除ckx_waibwl表时报错"+e.getMessage());
		}
	}

	/**
	 * 行记录解析之后   给行记录增加创建人、创建时间、修改人、修改时间
	 * */
	@Override
	public boolean afterRecord(Record record) {	
		record.put("create_time",date);
		record.put("creator",interfaceId);
		record.put("editor",interfaceId);
		record.put("edit_time",date);
		return true;
	}

    @Override
    public void after() {
        //对主表标记editor为temp
        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
                .execute("inPutzxc.waibwlUpdateTemp");
        //对主表与中间表进行合并
        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
                .execute("inPutzxc.waibwlMerge");
        //删除主表标记editor为temp的数据
        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
                .execute("inPutzxc.waibwlDeleteTemp");
    }
}
