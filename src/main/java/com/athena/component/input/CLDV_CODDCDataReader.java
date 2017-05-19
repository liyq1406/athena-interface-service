package com.athena.component.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.db.ConstantDbCode;
import com.athena.util.exception.ServiceException;


/**
 * 3040 CLDV-CODDC对应关系
 * @author hzg
 *
 */
public class CLDV_CODDCDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(CLDV_CODDCDataReader.class);	//定义日志方法
	public CLDV_CODDCDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);

	}
	
	/**
	 * 解析数据之前清空in_cldv_coddc表数据
	 */
	@Override
	public void before() {
		/*
		try{ 
			//检查pds传来的文件的完整性
			String readerConfigPath = dataParserConfig.getReaderConfig().getFilePath();
	    	int top = readerConfigPath.lastIndexOf("/");
	    	String filePath = readerConfigPath.substring(0,top);
	    	String fileName = readerConfigPath.substring(top+1,readerConfigPath.length());
	    	File file = new File(filePath);
	    	File[] in_files = file.listFiles();
	    	if(in_files!=null){
	    		for(File f:in_files){
	        		if(isContainsFile(f.getName(), fileName)){
	        		        checkFileEnd(f,"UTF-8");
	        		}
	        	}
	    	}
	    	
		}catch(ServiceException e)
		{
			logger.error("线程--接口" + dataParserConfig.getId() +"检查开始，结束标识符失败，"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"检查开始，结束标识符失败，"+e.getMessage());
		}
		*/		
		try{
			String i = "";//成功次数
			String k = "";//失败次数 
			String lastffn=""; //最后一次失败执行的文件名
			String lastsfn=""; //最后一次成功执行的文件名	
			String file_name ="";//InFileType文件名
			
			int j=0;//成功余数
			int m=0;//失败余数
			int wjgs=0;//文件个数
			
			Map<String,String> params  = new HashMap<String,String>();
			Map<String,String> params2  = new HashMap<String,String>();
			String FILE_SATUS = "";
			String FILE_NAME = "";
			params.put("inbh","3040");
			
			//上次的执行日志，不带状态 0012665
			List<Map<String,String>> flist = dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId()).select("inPutddbh.queryLastFilelog",params); 
			
			if(flist!= null && flist.size()>0){	
				params2 = flist.get(0);//最新一行	
				 FILE_SATUS = params2.get("FILE_SATUS").toString();
				 FILE_NAME = params2.get("FILE_NAME").toString();
			}
			
			i =(String) dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.selectObject("inPutddbh.queryFileNumer",params);
			logger.error("线程--接口" + dataParserConfig.getId() +"执行记录数:"+i);
			
			k =(String) dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.selectObject("inPutddbh.queryFiledFileNumer",params);
			logger.error("线程--接口" + dataParserConfig.getId() +"执行记录数:"+k);	
			
			lastffn =(String) dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.selectObject("inPutddbh.queryFileFiledName",params);
			logger.error("线程--接口" + dataParserConfig.getId() +"最后失败执行文件名:"+lastffn);
			
			if (lastffn==null){
				lastffn="";
			} 
			
			lastsfn =(String) dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.selectObject("inPutddbh.queryFileName",params);
			logger.error("线程--接口" + dataParserConfig.getId() +"最后成功执行文件名:"+lastsfn);
			
			if (lastsfn==null){
				lastsfn="";
			} 
				
			file_name =(String) dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.selectObject("inPutddbh.findInFileType",params);					
			
			String[] filestrs = file_name.split(",");
			wjgs = filestrs.length;
			logger.error("线程--接口" + dataParserConfig.getId() +"文件个数:"+wjgs);	  
			
			if(i.equals("0")&&k.equals("0")){//初次运行
				dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				.execute("inPutddbh.cldv_coddcDelete");
				
				//清空in_cldv_coddc_t表
		        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				.execute("inPutddbh.deleteCoddc_t");	
			}else {
				if(!k.equals("0")){//有错误的运行记录		
			     if(wjgs == 1){
				      dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
					  .execute("inPutddbh.cldv_coddcDelete");	
				      
						//清空in_cldv_coddc_t表
				        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						.execute("inPutddbh.deleteCoddc_t");
				 }else{
				     if(lastsfn==null || lastsfn.equals("")){//今天没有成功运行记录时，
	                       dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						   .execute("inPutddbh.cldv_coddcDelete");	
	                       
	           			//清空in_cldv_coddc_t表
	       		        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
	       				.execute("inPutddbh.deleteCoddc_t");
					 }
				     
					 //如果上次执行的文件时 ipds04，不管状态，本次清除
					 if(FILE_NAME.contains("ipds04") ){
					     dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
					      .execute("inPutddbh.cldv_coddcDelete");	
					     
							//清空in_cldv_coddc_t表
					        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
							.execute("inPutddbh.deleteCoddc_t");
					 }
					 //如果上次执行的是ipds02,且状态是失败则清除
					 if( FILE_NAME.contains("ipds02") && FILE_SATUS.equals("-1") ){ 
					     dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
					      .execute("inPutddbh.cldv_coddcDelete");	
					     
							//清空in_cldv_coddc_t表
					        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
							.execute("inPutddbh.deleteCoddc_t");
					  }	 
				     
				  }	
		    }else{//无错误的运行记录,多次运行
			   if(wjgs == 1){
				     dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				      .execute("inPutddbh.cldv_coddcDelete");	
				     
						//清空in_cldv_coddc_t表
				        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						.execute("inPutddbh.deleteCoddc_t");
			    }else{
				     if(lastsfn.contains("ipds04")){
	                       dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						   .execute("inPutddbh.cldv_coddcDelete");
	                       
	           			//清空in_cldv_coddc_t表
	       		        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
	       				.execute("inPutddbh.deleteCoddc_t");
						}	      
				  }
			}
		  }
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + dataParserConfig.getId() +"清除in_cldv_coddc表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"清除in_cldv_coddc表时报错"+e.getMessage());
		}
	}

	/**
	 * 行解析前处理
	 */
	@Override
	public boolean beforeRecord(String line, String fileName, int lineNum){
		boolean result = true;
		if (lineNum==1) {// 文件第一行不导入表
			result = false;
		}
		if(StringUtils.isEmpty(line.toString().trim())){ //如果为空字符行过滤掉
			result = false;
		}
		if(line!=null&&line.toString().contains("PDS—ATHENA")){
			result = false;
		}
		return result;
	}
	
	/**
	 * 接口执行完后处理
	 *  mantis:0010734
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void after() {
		/*保留最后一个相同的Lcdv1424、Coddc
        dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
		   .execute("inPutddbh.deleteDoubleLcdvCoddc");	
		*/
		
	}
		
}
