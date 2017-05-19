package com.athena.component.output;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Date;
import java.math.BigDecimal;

import org.dom4j.Element;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputXmlSerivce;

import com.ibm.mq.MQQueueManager;
import com.athena.component.service.utls.MQConfig;
import com.athena.component.service.utls.MQService;

/*xss
 * 2015-12-28
 * 调拨申请发送DFPV
 * */
public class DiaoBoSqXmlWriter extends DBOutputXmlSerivce{
	public DiaoBoSqXmlWriter(DataParserConfig dataParserConfig) {
	}  
	
	/*接口执行前方法*/
	public  void before(){ 
    	//更新调拨单明细的数据状态为‘70-已完成’
		int num1c = baseDao.getSdcDataSource(sourceId).execute("outPut.updateDiaobdZhangt70");
		//logger.info("接口" + dataParserConfig.getId() + "更新"+num1c+"条已完成的调拨单明细数据！"); 

    	//更新调拨申请明细的数据状态为‘70-已完成’
		int num1b = baseDao.getSdcDataSource(sourceId).execute("outPut.updateDiaobSqmxZhangt70");
		//logger.info("接口" + dataParserConfig.getId() + "需要回传"+num1b+"条已完成的调拨申请明细数据！");		
		
    	//将已经发送并执行、并且审核通过的调拨数据 更改为‘70-已完成’，beiz1改为‘0’，再次发送。
		int num1a = baseDao.getSdcDataSource(sourceId).execute("outPut.updateDiaobSqZhangt70");
		logger.info("接口" + dataParserConfig.getId() + "需要回传"+num1a+"条已审核完成的调拨数据！");		
    	
    	//将已经发送并终止、但未有调拨流水的数据， beiz1更改为‘0’，再次发送。
		int num2 = baseDao.getSdcDataSource(sourceId).execute("outPut.updateDiaobSqZhangt60");
		logger.info("接口" + dataParserConfig.getId() + "需要回传"+num2+"条已终止的调拨数据！");		
	}
	
	
    /*修改报文头内容*/
    public  void afterHead (Element RequestHead){
    	Element ServiceOperation = RequestHead.element("ServiceOperation");   
    	ServiceOperation.setText("ATHS04");
    	logger.info("接口" + dataParserConfig.getId() + "修改报文头内容成功！");
    }

    /*修改报文头体内容*/
    public  void afterBody (Element RequestBody){  
		Element AppRequest = RequestBody.element("AppRequest");
		Element AppReqHead = AppRequest.element("AppReqHead");
		Element TradeCode = AppReqHead.element("TradeCode");
		TradeCode.setText("ATHS04");  
    	logger.info("接口" + dataParserConfig.getId() + "修改报文体内容成功！");	
    }
    
	/*接口完成后方法*/
	public  void after(){
    	//将状态为'30-已生效'的调拨申请单数据 改为'40-执行中'
		int num3a = baseDao.getSdcDataSource(sourceId).execute("outPut.updateDiaobSqZhangt30");
		logger.info("接口" + dataParserConfig.getId() + "更新"+num3a+"条调拨申请单数据为执行中！");  
		
    	//将状态为'30-已生效'的调拨申请单明细数据 改为'40-执行中'
		int num3b = baseDao.getSdcDataSource(sourceId).execute("outPut.updateDiaobSqmxZhangt30");
		logger.info("接口" + dataParserConfig.getId() + "更新"+num3b+"条调拨申请单明细数据为执行中！");  
		
    	//将状态为'30-已生效'的调拨单数据 改为'40-执行中'
		int num3c = baseDao.getSdcDataSource(sourceId).execute("outPut.updateDiaobdZhangt30");
		logger.info("接口" + dataParserConfig.getId() + "更新"+num3c+"条调拨单数据为执行中！"); 
		
		
    	//将状态为'40-执行中'的数据 改为'50-已执行'
		int num4a = baseDao.getSdcDataSource(sourceId).execute("outPut.updateDiaobSqZhangt50");
		logger.info("接口" + dataParserConfig.getId() + "更新"+num4a+"条调拨申请单数据为已执行！"); 
		
    	//将状态为'40-执行中'的数据 改为'50-已执行'
		int num4b = baseDao.getSdcDataSource(sourceId).execute("outPut.updateDiaobSqmxZhangt50");
		logger.info("接口" + dataParserConfig.getId() + "更新"+num4b+"条调拨申请单明细数据为已执行！"); 
		
    	//将状态为'40-执行中'的数据 改为'50-已执行'
		int num4c = baseDao.getSdcDataSource(sourceId).execute("outPut.updateDiaobdZhangt50");
		logger.info("接口" + dataParserConfig.getId() + "更新"+num4c+"条调拨明细单数据为已执行！"); 
		
		
    	//将所有DFPV 的beiz1为'0'或者空的 数据打标BEIZ1为’1-已发送‘
		int num5 = baseDao.getSdcDataSource(sourceId).execute("outPut.updateDiaobSqZhangt"); 
		logger.info("线程--接口" + interfaceId +" 共发送条数："+num5+"条调拨申请数据");	 
		
		
		//0012554 
		Map<String,String> bean = new HashMap<String,String>();
		bean.put("leix", "1");
		bean.put("zhuangt", "50"); 
		
		//挑选所有的DFPV类型并且状态为50调拨明细数据
		List<Map<String,Object>> dbmxList =  dataParserConfig.getBaseDao().getSdcDataSource(sourceId).select("outPut.queryDfpvDiaobmx50",bean);	
		
		for(Map<String,Object> dbmx:dbmxList){
			BigDecimal chae = BigDecimal.ZERO;
			String shengbd = "";
			BigDecimal yzxsl =  BigDecimal.ZERO;
			BigDecimal shipsl =  BigDecimal.ZERO;
			BigDecimal sumshul = BigDecimal.ZERO;
			
			String cangkbh = (String) dbmx.get("CANGKBH");
			String zickbh = (String)dbmx.get("ZICKBH");
			shengbd = cangkbh ;
			//shengbd = cangkbh.concat(zickbh);
			
			if ( (BigDecimal)dbmx.get("ZHIXSL") != null ){
				yzxsl =  (BigDecimal)dbmx.get("ZHIXSL");//原执行数量
			}
			
			if ( (BigDecimal)dbmx.get("SHIPSL") != null ){
				shipsl = (BigDecimal)dbmx.get("SHIPSL");//实批数量
			} 
			
			bean.put("diaobdh", (String) dbmx.get("DIAOBDH"));
			bean.put("diaobsqdh", (String) dbmx.get("DIAOBSQDH"));
			bean.put("lingjbh", (String) dbmx.get("LINGJBH" ));
			bean.put("usercenter", (String) dbmx.get("USERCENTER") );
			
			//按照调拨单号，零件，用户中心汇总dfpv最新的执行数量
			String tmp  = "";
			tmp = (String) dataParserConfig.getBaseDao().getSdcDataSource(sourceId).selectObject("outPut.SumDfpvDbZxshul",bean);
			sumshul =  new BigDecimal(tmp);	
			
			//用汇总的执行数量进行比对，如果和原执行数量不符，则进行更新
			if( sumshul.compareTo(yzxsl) != 0 ){
				//更新dbmx中的执行数量
				dbmx.put("SUMSHUL", sumshul); 
				int gxcouts = baseDao.getSdcDataSource(sourceId).execute("outPut.updateDfpvZxsl",dbmx); 
				
				logger.info("线程--接口" + interfaceId +"更新："+gxcouts+"条执行数量");	
				
				if( sumshul.compareTo(shipsl) != 0 ){
					chae = sumshul.subtract(shipsl) ;//本次差额数量 = 汇总执行数量-实批数量
				}else{
					chae = shipsl.subtract(yzxsl) ;//本次差额数量 = 实批数量-上次执行数量 
				}
				
				if( chae.compareTo(sumshul) != 0 ){ //差额不等于汇总数量时
					dbmx.put("CHAE", chae); 
					dbmx.put("SHENGBD", shengbd);
					dbmx.put("FLAG", "1"); 
					dbmx.put("CAOZLX", "1"); 
					
					//将比对的差额 ，插入ck_yicsbcz表
					baseDao.getSdcDataSource(sourceId).execute("outPut.insert_yicsbcz", dbmx);					
				} 							
			}
		}			
	}
	
	public  void send(ExchangerConfig[] ecs){
		for (ExchangerConfig ec : ecs) {
            String encoding = ec.getEncoding();
            
            String fileName = ec.getFileName();
            fileName = fileName.replaceAll("txt", "xml"); 
            
            //默认为GBK
            if(encoding==null){
                encoding = "utf-8";
            }
            
            String filepath = ec.getAthfilePath() + File.separator + fileName;
            logger.info("线程--接口" + interfaceId +"文件路径："+filepath);	   
            
    		try {
    			MQService ms = new MQService(); 
    			MQConfig config = MQConfig.getNewDbConfigFromKey();
    			MQQueueManager qm = null;
    			qm = ms.createMQmanage(config.getMQ_HOST_NAME(), config.getMQ_PROT(), config.getMQ_CHANNEL(), config.getMQ_MANAGER(), config.getMQ_CCSID());
    			File file = new File(ec.getAthfilePath() + File.separator + fileName);
    			ms.sendMessageFile(file, qm, config.getMQ_MANAGER(), config.getMQ_QUEUE_NAME());
			} catch (Exception e) {
				logger.error("给DFPV发给文件出错"+e.getMessage());
			} 
		}
	}
		
	 	
		
}
	

   