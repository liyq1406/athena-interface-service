package com.athena.component.input;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;
import com.toft.core2.DBException;

public class GdtbxqDBDataWriter extends TxtInputDBSerivce{
	private String datasourceid;
	protected static Logger logger = Logger.getLogger(GdtbxqDBDataWriter.class);	//定义日志方法
	public GdtbxqDBDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceid = dataParserConfig.getReaderConfig().getDatasourceId();
	}
	
	/**
	 * 执行前将sppv.sppv034表中的JSBS='F'改为'J'
	 * 2013-10-9
	 */
	@Override
	public void before(){
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceid)
			.execute("inPutzxc.updateSppv034JsbsToJ");
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"before更新sppv034表JSBS状态F为J时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"before更新sppv034表JSBS状态F为J时报错"+e.getMessage());
		}
	}
	
	
	@Override
	public boolean afterRecord(Record record){
		record.put("LMP", record.getString("USERCENTER")+"5"+record.getString("LMP"));
		String xiaohdbh = queryXiaohdbhOfpeislb(record);
		String danw = queryDanwOfLingj(record);
		record.put("XIAOHDBH",StringUtils.isEmpty(xiaohdbh)?"":xiaohdbh.substring(0,9));
		record.put("DANW", danw== null ? "":danw);
		record.put("JSBS", "F");
		return true;
	}

	/**
	 * 查询配送类别获取消耗点编码
	 * @author 贺志国
	 * @date 2013-2-27
	 * @param record
	 * @return
	 */
	private String queryXiaohdbhOfpeislb(Record record) {
		String xiaohdbh = "";
		Map<String,String> params = new HashMap<String,String>();
		params.put("USERCENTER",record.getString("USERCENTER"));
		params.put("PRODUCT",record.getString("PRODUCT"));
		params.put("LMP",record.getString("LMP"));
		params.put("OCLASS",record.getString("OCLASS"));
		try {
			xiaohdbh= (String)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.selectObject("inPutzxc.querySppvXiaohdbh", params);
		} catch (DBException e) {
			logger.error(e.getMessage());
		}
		return xiaohdbh;
	}
	
	/**
	 * 查询参考系零件表ckx_lingj取零件单位
	 * @author 贺志国
	 * @date 2013-2-27
	 * @param record
	 * @return
	 */
	private String queryDanwOfLingj(Record record){
		String danw = "";
		Map<String,String> params = new HashMap<String,String>();
		params.put("USERCENTER",record.getString("USERCENTER"));
		params.put("PRODUCT",record.getString("PRODUCT"));
		try {
			danw = (String)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.selectObject("inPutzxc.querySppvDanw", params);
		} catch (DBException e) {
			logger.error(e.getMessage());
		}
		return danw;
	}
	
	/**
	 * 更新sppv sppv034表JSBS为'T'(已处理状态)
	 */
	@Override
	public void after() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceid)
			.execute("inPutzxc.updateSppv034");
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"after更新sppv034表JSBS状态J为T时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"after更新sppv034表JSBS状态J为T时报错"+e.getMessage());
		}
	}
	

}
