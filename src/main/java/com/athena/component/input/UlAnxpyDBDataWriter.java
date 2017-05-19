package com.athena.component.input;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.component.exchange.utils.ConvertUtils;
import com.athena.util.exception.ServiceException;
/**
 * 0011505 按需平移 UL
 * 1890
 * xss
 *
 */
public class UlAnxpyDBDataWriter extends TxtInputDBSerivce{
	public String datasourceId = "";
	public String usercenter = "'UL','UX'";	
	public String inbh = "1890";
	
	public UlAnxpyDBDataWriter(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	

	/**
	 * 清空临时表数据
	 */
	@Override
	public void before() {
		try{//清空2天前的数据
			Map<String,Object> param = new HashMap<String,Object>();
			param.put("usercenter", usercenter);
			dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.execute("inPutzbc.anxpytempDelete",param);
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + interfaceId +"清除xqjs_anxmaoxq_temp表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"清除xqjs_anxmaoxq_temp表时报错"+e.getMessage());
		}
	}
	
	/**
	 * 接口处理完成后更新xqjs_anxmaoxq、xqjs_anxmaoxq_temp表
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void after() {
			try{				
				Map<String,Object> param1 = new HashMap<String,Object>();
				Map<String,String> param2 = new HashMap<String,String>();
				//查询维护的参数
				
				param2.put("usercenter", usercenter);
				param2.put("inbh", inbh);
				
				String k =(String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutzbc.countAnxpy",param2);		
				
				if(!k.equals("0")||!k.equals(null) ){ 
				
					//筛选用户中心和产线 如:多条产线
					List<Map<String,Object>> uchanxlist = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzbc.queryAnxpychx",param2); 										
					
					//循环用户多产线
					for(Map<String,Object> uchanx : uchanxlist){
							//查询产线的最新维护参数
							uchanx.put("inbh", inbh);
						
							List<Map<String,Object>> plist = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzbc.queryAnxpy",uchanx); 
										
							if(plist!= null && plist.size()>0){	
							param1 = plist.get(0);//最新一行					
							
							String usercenter = param1.get("USERCENTER").toString();
							String chanx = param1.get("CHANX").toString();
							String txsj = param1.get("TXSJ").toString();	
							int pysj = Integer.parseInt(param1.get("PYSJ").toString());	
							//String pysj = param1.get("PYSJ");		
																	
							//筛选xqjs_anxmaoxq_temp表今天 emon<=停线时间 and 消耗时间=>停线时间的sppv数据
							List<Map<String,Object>> listUwSppv = dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId()).select("inPutzbc.queryAnxpySppv",param1);		
							
							//循环符合条件的数据列表
							for(Map<String,Object> uwsppv : listUwSppv){							
								uwsppv.put("PYSJ", String.valueOf(pysj));
							
								//根据用户中心、产线、消耗时间、平移时间 算出 新消耗时间
								String newxhsj = (String)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId()).selectObject("inPutzbc.queryNewxhsj",uwsppv);
								
								if(newxhsj ==null||newxhsj.equals("")){
									logger.error("线程--接口" + interfaceId +"推算新消耗时间失败"+uwsppv.get("CHANX").toString());	
									continue;
								}
								//新消耗时间
								uwsppv.put("NEWXHSJ", newxhsj);	
								
								uwsppv.put("inbh", inbh);	
								try{
									//更新xqjs_anxmaoxq表的新消耗时间
									dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
									.execute("inPutzbc.updateNewxhsjSppv",uwsppv);
								}catch(RuntimeException e){
									logger.error("线程--接口" + interfaceId +"更新xqjs_anxmaoxq_temp表的消耗时间"+e.getMessage());
									throw new ServiceException("线程--接口" + interfaceId +"更新xqjs_anxmaoxq表的消耗时间"+e.getMessage());
								}
								
								//更新xqjs_anxmaoxq_temp 表的flag
								dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
								.execute("inPutzbc.updateFlag",uwsppv);	

							}
							
							//更新ckx_anxpy参数状态
							dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
							.execute("inPutzbc.updateCkxFlag",param1);								
						  }
				}
												
				} 				
			}catch(RuntimeException e){
				logger.error("线程--接口" + interfaceId +"更新xqjs_anxmaoxq_temp表时报错"+e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId +"更新xqjs_anxmaoxq_temp表时报错"+e.getMessage());
			}
	}

}
