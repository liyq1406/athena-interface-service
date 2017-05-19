package com.athena.component.output;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
 * BOM零件切换汇总
 * @author CSY
 * @date 2016-05-17
 *
 */

public class BOMlingjhzDBDataWriter extends DBOutputTxtSerivce {

	public BOMlingjhzDBDataWriter(DataParserConfig dataParserConfig) {
		
	}
		
	/**
	 * 2016-05-16
	 * add by CSY
	 * 运行前先将反馈表中flag3数据为0（未执行）的数据状态改为1（执行中）
	 */
	@Override
	public void before() {
		logger.info("线程--接口" + interfaceId +" 更新反馈表状态");	
		int num = dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.updateFlag3QHFK0to1");
		logger.info("线程--接口" + interfaceId +" 更新反馈表状态结束，共更新"+num+"条");
		
	}
	
	/**
	 * 2016-05-16
	 * add by CSY
	 * 运行后将反馈表中flag3数据为1（执行中）的数据状态改为2（执行完）
	 */
	@Override
	public void after() {
		logger.info("线程--接口" + interfaceId +" 更新反馈表状态");	
		int num = dataParserConfig.getBaseDao().getSdcDataSource(sourceId).execute("outPut.updateFlag3QHFK1to2");
		logger.info("线程--接口" + interfaceId +" 更新反馈表状态结束，共更新"+num+"条");
		
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
	     	before();
	     	for (ExchangerConfig ec : ecs) {
	       	outPut(ec, readerConfig);
	       	fileName.append(ec.getFileName()).append(",");
	       	}
	     	after();
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
		int pageSize = getPageSize();
		int totalPage = getTotalPage(write.getUsercenter(), read.getSql(),read.getIsAllSet(),pageSize);
	  	try {
	    	
	    	int time = 0;
	        for(int i=0;i<totalPage;i++){
	            int endPage = (i+1)*pageSize;  //结束页数条数
	            int startPage = i*pageSize+1;     //开始页数条数
	            String head = "select * from ("; 
	            String foot =" and rownum <= "+endPage+") t"
	                			+" where t.RN >= "+startPage ;
	            Map<String,String> params = putParams(write.getUsercenter(),head,foot.toString(),read.getIsAllSet()); //分页参数替换
	            List<Map<String,Object>> dataList = baseDao.getSdcDataSource(sourceId).select(read.getSql(), params);

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
		    	TargetSystem.setText("BOM");
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
		    	TableElement.addAttribute("name", "BOM零件切换");
		    		
		    	Element rowsElement = TableElement.addElement("rows");
	            
	            TableElement.addAttribute("num", ""+dataList.size());
	      		/*********************************************************/
	            for (int j = 0; j < dataList.size(); j++) {
	            	
		            //row
		        	Element rowElement = rowsElement.addElement("row");
		        	rowElement.addAttribute("BANBH", dataList.get(j).get("BANBH").toString().trim());
		        	rowElement.addAttribute("TOUFLWJH", dataList.get(j).get("TOUFLWJH").toString().trim());
		        	rowElement.addAttribute("LIUSH", dataList.get(j).get("LIUSH").toString().trim());
		        	rowElement.addAttribute("LINGJBH", dataList.get(j).get("LINGJBH").toString().trim());
//		        	rowElement.addAttribute("YZHENGF", dataList.get(j).get("TZA").toString().trim());
		        	rowElement.addAttribute("YSHUL", dataList.get(j).get("YSHUL").toString().trim());
		        	rowElement.addAttribute("XLINGJBH", dataList.get(j).get("XLINGJBH").toString().trim());
//		        	rowElement.addAttribute("XZHENGF", dataList.get(j).get("TZB").toString().trim());
		        	rowElement.addAttribute("XSHUL", dataList.get(j).get("XSHUL").toString().trim());
		        	rowElement.addAttribute("SHIJQHRQ", dataList.get(j).get("SHIJQHRQ").toString().trim());
		        	rowElement.addAttribute("BEIZ", "");
				}
	            time ++ ;
	            /*********************************************************/
	            try {
	        		OutputFormat format = OutputFormat.createPrettyPrint();
	        		format.setEncoding("utf-8");
	        		String fileUrl1 = write.getFilePath()+"/"+sdf.format(date)+"_"+time+"_"+write.getFileName().substring(0, write.getFileName().lastIndexOf("."));
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
	        			//File file = new File("E:\\Users\\ath00\\tmp\\"+write.getFileName().substring(0, write.getFileName().lastIndexOf(".")));
	        			ms.sendMessageFile(file, qm, config.getMQ_MANAGER(), config.getMQ_QUEUE_NAME());
	        		} catch (Exception e) {
						logger.error("给DFPV发给文件出错"+e.getMessage());
						throw new ServiceException("接口" + dataParserConfig.getId() + "给DFPV发给文件出错", e);
					}
	        	} catch (IOException e) {   
	        		logger.error("接口" + dataParserConfig.getId() + "IO输出异常", e);
	                throw new ServiceException("接口" + dataParserConfig.getId() + "IO输出异常", e);
	        	}
		        
	        }
	        
	    } finally {
	    }
	 }
	 


	@Override
	public void afterAllRecords(ExchangerConfig[] ecs) {
	}
	
	/**
	 * 得到配置文件的分页数
	 * @author csy
	 * @date 2016-01-15
	 * @return pageSize
	 */
	@SuppressWarnings("unchecked")
	private int getPageSize(){
		InputStream  in = BOMlingjhzDBDataWriter.class.getResourceAsStream("/config/exchange/urlPath.properties");
		Properties pp = new Properties();
		int pageSize = 0;
		try {
			pp.load(in);
			pageSize = Integer.parseInt(pp.getProperty("pageSizeBOM"));
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return pageSize;
	}

	/**`
     * 计算分多少页
     */
	private int getTotalPage(String usercenter, String sql,String flagStr,int pageSize) throws SQLException{
        int totalNum = getTotalNum(usercenter, sql,flagStr);
        int totalPage  = totalNum/pageSize + (totalNum%pageSize==0 ? 0 : 1);
        return totalPage;
    }


    /**
     *
     * @param usercenter
     * @param sqlId
     * @param flagStr
     * @return
     * @throws SQLException
     */
	private int getTotalNum(String usercenter,String sqlId,String flagStr) throws SQLException{
    	int countNum = 0;
        String head = "select count(1) COUNTNUM from (";
        String foot = " ";
        Map<String,String> params = putParams(usercenter, head,foot.toString(),flagStr); //分页参数替换
        Map dataMap= (Map)baseDao.getSdcDataSource(sourceId).selectObject(sqlId, params);
        countNum = Integer.parseInt(dataMap.get("COUNTNUM").toString());
        return countNum;
    }

	
}
