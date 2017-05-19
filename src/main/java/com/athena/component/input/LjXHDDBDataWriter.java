package com.athena.component.input;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.exception.ServiceException;

/**
 * 2090 零件-消耗点
 * @author hzg
 *
 */
public class LjXHDDBDataWriter extends TxtInputDBSerivce {
	private String datasourceId = "";
	public LjXHDDBDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId=dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	/**
	 * 行解析之后处理方法
	 * 初始值将创建人改为new
	 */
	@Override
	public boolean afterRecord(Record record) {
		//准备层过来的数据biaos如果为1，执行层原有数据biaos为0，则更四个总量与准备层相同 hzg 2013-7-22
		//0013552：修改为准备层过来的数据，若果执行层原有数据biaos为0，则更四个总量与准备层相同 lc 2017-4-11
		//if("1".equals(record.getString("BIAOS"))){
			//根据主键USERCENTER,LINGJBH,XIAOHDBH和ckx_lingjxhd表biaos状态去做更新 hzg 2013-7-22
			Map<String,String> params = new HashMap<String,String>();
			params.put("USERCENTER", record.getString("USERCENTER"));
			params.put("LINGJBH", record.getString("LINGJBH"));
			params.put("XIAOHDBH", record.getString("XIAOHDBH"));
			params.put("YIFYHLZL", record.getString("YIFYHLZL"));
			params.put("JIAOFZL",record.getString("JIAOFZL"));
			params.put("XITTZZ", record.getString("XITTZZ"));
			params.put("ZHONGZZL", record.getString("ZHONGZZL"));
			updateLingjxhdOfZongl(params);
			
			//}
		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", new Date());
		record.put("EDITOR", interfaceId);
		record.put("EDIT_TIME", new Date());
		try {
			record.put("SHENGXR",DateTimeUtil.StringYMDToDate(record.get("SHENGXR").toString()));
			record.put("JIESR",DateTimeUtil.StringYMDToDate(record.get("JIESR").toString()) );
		}catch(ParseException e){
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		} 
		return true;
	}
	
	/**
	 * 更新ckx_lingjxhd表中biao为0的数据的四个总量的值
	 * @author Hezg
	 * @date 2013-7-22
	 * @param USERCENTER 用户中心
	 * @param LINGJBH 零件编号 
	 * @param XIAOHDBH 消耗点编号
	 */
	public void updateLingjxhdOfZongl(Map<String,String> params){
		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.updateLingjxhdOfZongl",params);
	}
	
	
	
	/**
	 * 处理后的调用
	 * 将小火车信息写到小火车表中
	 * hzg 2013-12-11
	 */
	/*public void after(){
        try{
        	//1 、根据用户中心，产线，小火车编号分组查询出小火车信息写到小火车表
        	dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.insertXiaohcinfoOfLingjxhdIntoXiaohc");
        }catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"将ckx_lingjxhd表的小火车信息写到ckx_xiaohc表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"将ckx_lingjxhd表的小火车信息写到ckx_xiaohc表时报错"+e.getMessage());
		}
	}*/
}
