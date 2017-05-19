package com.athena.component.input;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;
/**
 *1780 更新订单表中订单生效时间
 * @author hzg
 * @date 2013-7-26
 */
public class DingdSxsjDbDataReader extends TxtInputDBSerivce{
	private String datasourceId = "";
	public DingdSxsjDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	/**
	 * 查询xqjs_dingd表订单号为C1DDD0008 ，用户中心为UL的记录是否存在
	 * 不存则抛出异常
	 */
	public void before(){
		try{
			String cnum = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).
			selectObject("inPutzbc.queryCountOfDingd");
			if("0".equals(cnum)){
				throw new ServiceException("线程--接口" + interfaceId +" xqjs_dingd表中不存在订单号为C1DDD0008，用户中心为UL的订单");
			}
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +" 查询xqjs_dingd表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"查询xqjs_dingd表时报错"+e.getMessage());
		}
	}
	
	/**
	 * 行解析之后处理，只存在一条记录 
	 * 更新xqjs_dingdjssj表UL C1DDD0008 记录的订单生效时间
	 * update 2016.1.6
	 * record 行结果集
	 */
	@Override
	public boolean afterRecord(Record record){
		try {
			Map<String,String> params = new HashMap<String,String>();
			params.put("usercenter", record.getString("USERCENTER").toString());
			params.put("dingdh", record.getString("DINGDH").toString());
			String sxsj ="";
			SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.UK);
			SimpleDateFormat sf1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss",Locale.CHINA);
			try {
				java.util.Date date = sdf.parse(record.getString("DINGDSXSJ").toString());
				 sxsj = sf1.format(date);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			params.put("dingdsxsj", sxsj);
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.updateDingdsxsj", params);
		} catch (RuntimeException e) {
			logger.error("线程--接口" + interfaceId +" 查询xqjs_dingdjssj表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"查询xqjs_dingdjssj表时报错"+e.getMessage());
		}
		return true;
	}
	
	

}
