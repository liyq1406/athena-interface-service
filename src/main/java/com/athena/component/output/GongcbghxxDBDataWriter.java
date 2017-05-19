package com.athena.component.output;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.component.service.utls.MQConfig;
import com.athena.component.service.utls.MQService;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;
import com.ibm.mq.MQQueueManager;

/**
 * 工程变更号
 * @author CSY
 * @date 2015-12-31
 */

public class GongcbghxxDBDataWriter extends DBOutputTxtSerivce {
	
	public GongcbghxxDBDataWriter(DataParserConfig dataParserConfig) {
		
	}
		
	
	@Override
	public boolean  write(DataParserConfig dataParserConfig) throws ServiceException {
		logger.info("接口" + dataParserConfig.getId() + "开始输出");
	    //根据配置获得SQL或SQLID
	    boolean flag = false;
	    StringBuffer fileName = new StringBuffer();
	    file_begintime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
	    interfaceId = dataParserConfig.getId();
	    this.dataParserConfig = dataParserConfig;
	    this.baseDao = dataParserConfig.getBaseDao();
	    try{
	    	ExchangerConfig readerConfig = dataParserConfig.getReaderConfig(); //得到reader的属性值
	    	this.dataFields = dataParserConfig.getDataFields(); // 得到配置文件字段
	       	sourceId = readerConfig.getDatasourceId();
	     	ExchangerConfig[] ecs = dataParserConfig.getWriterConfigs();
	     	for (ExchangerConfig ec : ecs) {
	       	outPut(ec, readerConfig);
	       	fileName.append(ec.getFileName()).append(",");
	       	}
	      	flag = true;
	   	} catch (SQLException sqlEx){
	   		logger.error(sqlEx.getMessage());
	       	throw new ServiceException(sqlEx);
	  	} catch(RuntimeException ex){
	     	logger.error(ex.getMessage());
	       	throw new ServiceException(ex);
	  	}finally{
	       	logger.info("接口" + dataParserConfig.getId() + "结束输出");
	      	file_info(fileName.toString(),flag);

	  	}
	  	return flag;
	 }
	
	 protected void outPut(ExchangerConfig write, ExchangerConfig read) throws SQLException{
		int totalPage = getTotalPage(write.getUsercenter(), read.getSql(),read.getIsAllSet());
	  	try {
	  	//XML文件AppReqBody之前数据
	      	//建立document对象，用来操作xml文件
	    	Document document = DocumentHelper.createDocument();  
	    	//建立根节点
	    	Element RootElement = document.addElement("Root");
	    	//添加一个Root子节点
	    	Element RequestHead = RootElement.addElement("RequestHead");
	    	//添加一个RequestHead子节点MessageType
	    	Element MessageType =  RequestHead.addElement("MessageType");
	    	//添加MessageType文本内容
	    	MessageType.setText("4");
	    	//添加一个RequestHead子节点SystemType
	    	Element SystemType = RequestHead.addElement("SystemType");
	    	//添加MessageType文本内容
	    	SystemType.setText("1");
	    	//添加一个RequestHead子节点SourceSystem
	    	Element SourceSystem = RequestHead.addElement("SourceSystem");
	    	//添加SourceSystem文本内容
	    	SourceSystem.setText("ATHENA");
	    	/*********************************************************/
	    	//添加一个RequestHead子节点TargetSystem 
	    	Element TargetSystem = RequestHead.addElement("TargetSystem");
	    	TargetSystem.setText("SAP");
	    	//添加一个RequestHead子节点ESBOutTime  
	    	Element ESBOutTime = RequestHead.addElement("ESBOutTime");
	    	ESBOutTime.setText(DateUtil.dateToStringYMDHms(new Date()));
	    	
	    	//2016-01-21新加入
	    	Element ServiceOperation = RequestHead.addElement("ServiceOperation");
	    	ServiceOperation.setText(write.getFileName().substring(0, write.getFileName().indexOf("."))); 
	    	
	    	//添加一个RequestHead子节点Version  
	    	Element Version = RequestHead.addElement("Version");
	    	Version.setText("1.0");
	   		//添加一个RequestHead子节点Version  
	   		Element Reserved = RequestHead.addElement("Reserved");
	   		Reserved.setText("1");

	    	Element RequestBody = RootElement.addElement("RequestBody");
	    	Element AppRequest = RequestBody.addElement("AppRequest");
	    	Element AppReqHead = AppRequest.addElement("AppReqHead");
	    	Element TradeCode = AppReqHead.addElement("TradeCode");
	    	//获取文件名
	    	TradeCode.setText(write.getFileName().substring(0, write.getFileName().indexOf("."))); 
	    	Element ReqSerialNo = AppReqHead.addElement("ReqSerialNo");
	    	//创建时间戳
	    	Date date = new Date();
	    	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	    	ReqSerialNo.setText(sdf.format(date));
	    	Element TradeTime = AppReqHead.addElement("TradeTime");
	    	TradeTime.setText(DateUtil.dateToStringYMDHms(new Date()));
	    	Element TradeDescription = AppReqHead.addElement("TradeDescription");
	    	TradeDescription.setText("");
	    	Element TradeLogLevel = AppReqHead.addElement("TradeLogLevel");
	    	TradeLogLevel.setText("1");
	    		 
	    	Element AppReqBody = AppRequest.addElement("AppReqBody");
	    	Element TableElement = AppReqBody.addElement("table");
	    	TableElement.addAttribute("id", write.getTable());
	    	
	    	/*********************************************************/
	    	TableElement.addAttribute("name", "工程变更号(输出)");
	    		
	    	int num = 0;
	    		
	    	Element rowsElement = TableElement.addElement("rows");
	        for(int i=0;i<totalPage;i++){
	            int endPage = (i+1)*PAGESIZE;  //结束页数条数
	            int startPage = i*PAGESIZE+1;     //开始页数条数
	            String head = "select * from ("; 
	            String foot =" and rownum <= "+endPage+") t"
	                			+" where t.RN >= "+startPage ;
	            Map<String,String> params = putParams(write.getUsercenter(),head,foot.toString(),read.getIsAllSet()); //分页参数替换
	            List<Map<String,Object>> dataList = baseDao.getSdcDataSource(sourceId).select(read.getSql(), params);
	        		 
	      		num += dataList.size();
	            
	      		/*********************************************************/
	            for (int j = 0; j < dataList.size(); j++) {
		            //row
		        	Element rowElement = rowsElement.addElement("row");
		        	rowElement.addAttribute("AENNR", dataList.get(j).get("AENNR").toString().trim());
		        	rowElement.addAttribute("AETXT", dataList.get(j).get("AENNR").toString().trim());
		        	rowElement.addAttribute("AENST", dataList.get(j).get("AENST").toString().trim());
		        	rowElement.addAttribute("DATUV", dataList.get(j).get("DATUV").toString().trim());
				}
	        }
	            
	        TableElement.addAttribute("num", ""+num);
	            
	    	/*********************************************************/
	        try {
        		OutputFormat format = OutputFormat.createPrettyPrint();
        		format.setEncoding("utf-8");
        		String fileUrl1 = write.getFilePath()+"/"+sdf.format(date)+"_"+write.getFileName().substring(0, write.getFileName().lastIndexOf("."));
        		String fileUrl2 = write.getAthfilePath()+"/"+write.getFileName().substring(0, write.getFileName().lastIndexOf("."));
        		XMLWriter xmlWriter1=new XMLWriter(new FileOutputStream(fileUrl1),format);
        		xmlWriter1.write(document);   
        		xmlWriter1.close();   
        		XMLWriter xmlWriter2=new XMLWriter(new FileOutputStream(fileUrl2),format);
        		xmlWriter2.write(document);   
        		xmlWriter2.close();
        		try {
        			logger.info("文件路径："+fileUrl2);
        			MQService ms = new MQService(); 
        			MQConfig config = MQConfig.getNewDbConfigFromKey();
        			MQQueueManager qm = null;
        			qm = ms.createMQmanage(config.getMQ_HOST_NAME(), config.getMQ_PROT(), config.getMQ_CHANNEL(), config.getMQ_MANAGER(), config.getMQ_CCSID());
        			File file = new File(fileUrl2);
        			ms.sendMessageFile(file, qm, config.getMQ_MANAGER(), config.getMQ_QUEUE_NAME());
        		} catch (Exception e) {
					logger.error("给DFPV发给文件出错"+e.getMessage());
					throw new ServiceException("接口" + dataParserConfig.getId() + "给DFPV发给文件出错", e);
				}
        	} catch (IOException e) {   
        		logger.error("接口" + dataParserConfig.getId() + "IO输出异常", e);
                throw new ServiceException("接口" + dataParserConfig.getId() + "IO输出异常", e);
        	}
	    } finally {
	    }
	 }
	 


	@Override
	public void afterAllRecords(ExchangerConfig[] ecs) {
	}
	
	

}
