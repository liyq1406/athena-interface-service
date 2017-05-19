package com.athena.component.input;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.utils.ConvertUtils;
import com.athena.db.ConstantDbCode;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;

public class NBYHLDBDataReader extends TxtInputDBSerivce{

	private String datasourceId = "";
	public NBYHLDBDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	/**
	 * 
	 */
	@Override
	public void after() {
		try{
			//查询备注7 为 1 的集合
			String diaobmxzt ="40";
			List<Map<String,String>> yaohllist = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzbc.yaohnblList");
			if(yaohllist!=null && yaohllist.size()>0){ 
				//0012554 记录调拨的异常消耗
				Map<String,String> bean = new HashMap<String,String>();
				bean.put("yaohlzt", "50");  
				
				//挑选所有状态为'已交付'调拨明细原始数据
				List<Map<String,Object>> dbmxList_tmp =  dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzbc.queryDiaobmx50_tmp",bean);	
				
				
				for (Map<String, String> map : yaohllist) {
					Map<String,String> params = new HashMap<String,String>();
					params.put("DINGDH", map.get("DINGDH"));
					params.put("CANGKBH", map.get("CANGKBH"));
					params.put("USERCENTER", map.get("USERCENTER"));
					params.put("LINGJBH", map.get("LINGJBH"));
					params.put("ZICKBH", map.get("ZICKBH"));
					List<Map<String,String>> list =  dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzbc.yaohnblcount",params);
					for (Map<String, String> map1 : list){
						if("00".equals(map1.get("YAOHLZT"))){
							if(String.valueOf(map1.get("ZTSL")).equals(String.valueOf(map1.get("ZSL")))){
								diaobmxzt="50";
							}
						}
						if("05".equals(map1.get("YAOHLZT"))){
							if(Integer.valueOf(String.valueOf(map1.get("ZTSL")))>0){
								diaobmxzt="60";
							}
						}
					}
					params.put("diaobmxzt", diaobmxzt);
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.updateyaohnblcount",params);
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.updatediaobmxcount",params);
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.updatediaobcounts",params);
					diaobmxzt="40"; 	 
				}  
				
				
				//挑选所有状态为'已交付'调拨明细更新后的数据
				List<Map<String,Object>> dbmxList =  dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzbc.queryDiaobmx50",bean);	
				
				for(Map<String,Object> dbmx:dbmxList){
					BigDecimal chae = BigDecimal.ZERO;
					String shengbd = "";
					BigDecimal yzxsl =  BigDecimal.ZERO;
					BigDecimal shipsl =  BigDecimal.ZERO;
					BigDecimal sumshul_t = BigDecimal.ZERO;
					BigDecimal sumshul = BigDecimal.ZERO;
					
					String usercenter  =  (String) dbmx.get("USERCENTER"); 
					String lingjbh  =  (String) dbmx.get("LINGJBH"); 
					
					String cangkbh = (String) dbmx.get("CANGKBH");
					String zickbh = (String)dbmx.get("ZICKBH"); 
					shengbd = cangkbh;
					
					String zhuangt = (String)dbmx.get("ZHUANGT"); 
					
					String yzhuangt="";
					for(Map<String,Object> dbmx_tmp:dbmxList_tmp){
						  if (  dbmx.get("DIAOBDH").equals(dbmx_tmp.get("DIAOBDH")) && 
								dbmx.get("LINGJBH").equals(dbmx_tmp.get("LINGJBH")) &&
								dbmx.get("USERCENTER").equals(dbmx_tmp.get("USERCENTER")) &&
								dbmx.get("CANGKBH").equals(dbmx_tmp.get("CANGKBH"))
							){
							if ( (BigDecimal)dbmx_tmp.get("ZHIXSL") != null ){
								yzxsl =  (BigDecimal)dbmx_tmp.get("ZHIXSL");//原执行数量
							}else{
								yzxsl =  BigDecimal.ZERO;
							}
							
							yzhuangt = (String) dbmx_tmp.get("ZHUANGT"); //原状态
						}  
					} 
					
					
					if ( (BigDecimal)dbmx.get("SHIPSL") != null ){
						shipsl = (BigDecimal)dbmx.get("SHIPSL");//实批数量
					}  
					
					//原状态
					//如果原状态是 60 ，现在又有50的状态的yaohhl 则不计算,进行跳过
					if(yzhuangt.equals("60")){
						continue;
					} 
					
					bean.put("dingdh", (String) dbmx.get("DIAOBDH"));
					bean.put("lingjbh", (String) dbmx.get("LINGJBH" ));
					bean.put("usercenter", (String) dbmx.get("USERCENTER") );
					bean.put("cangkbh", (String) dbmx.get("CANGKBH"));
					bean.put("zickbh", (String) dbmx.get("ZICKBH"));
					
					String tmp  = "";
					tmp = (String) dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutzbc.SumDbZxshul2",bean);
					sumshul_t =  new BigDecimal(tmp);
					
					//用汇总的执行数量进行比对，如果和原执行数量不符，则进行更新
					if( sumshul_t.compareTo(yzxsl) != 0 ){
						//二次交付
						if( yzxsl!=BigDecimal.ZERO || yzxsl.intValue()!=0 ){  
							//按照调拨单号，零件，用户中心查询本次的执行数量
							String tmp1  = "";
							tmp1 = (String) dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutzbc.SumDbZxshul",bean);
							sumshul =  new BigDecimal(tmp1);
							
							//如果上次状态是 40,本次是 50 则 ：修正数量 = 汇总执行数量-实批数量 
							if( yzhuangt.equals("40") && zhuangt.equals("50") ){
								chae = sumshul_t.subtract(shipsl) ;
							}else{//修正数量 = 实批数量-(实批数量-本次数量）
								chae = shipsl.subtract(shipsl.subtract(sumshul) ) ;
							} 
						}else{ 
							//一次交付
							//按照调拨单号，零件，用户中心汇总最新的执行数量
							String tmp2  = "";
							tmp2 = (String) dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutzbc.SumDbZxshul2",bean);
							sumshul =  new BigDecimal(tmp2);	
							 
							if( sumshul.compareTo(shipsl) != 0 ){
								chae = sumshul.subtract(shipsl) ;//本次修正数量 = 汇总执行数量-实批数量
							}else{
								chae = shipsl.subtract(yzxsl) ;//本次修正数量 = 实批数量-上次执行数量 
							}
						} 
						
						if( chae.compareTo(sumshul_t) != 0 ){ //差额不等于汇总数量时
							dbmx.put("usercenter", usercenter); 
							dbmx.put("yicxhl", chae); 
							dbmx.put("xiaohd", shengbd);
							dbmx.put("creator", interfaceId); 
							dbmx.put("lingjbh", lingjbh); 
							
							//将比对的差额 ，插入kucjscsb表
							dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.insert_kucjscsb", dbmx);					
						} 							
					}
				}
				
				//把备注7的值 更新为2 以示区别
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.updateyaohnblcounts");		
			}
		}catch(RuntimeException e){
			logger.error("线程--接口" + interfaceId +"更新CK_YAONBHL表中的要货类别为 调拨的"+e.getMessage(),e);
			throw new ServiceException("线程--接口" + interfaceId +"更新CK_YAONBHL表中的要货类别为 调拨的"+e.getMessage());
		}
	}
	
}