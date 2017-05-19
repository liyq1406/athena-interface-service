package com.athena.component.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 1080消耗点参考系 解析数据之前的处理方法
 * @date 2013-3-6
 * @author hzg
 */
public class XXDDbDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(XXDDbDataReader.class);	//定义日志方法
	public XXDDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
	}
	
	
	/**
	 * 解析数据之前更新ckx_gongzsjmb表数据，set d.gongybs=0
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
			
			int j=0;//成功余数
			int m=0;//失败余数
			
			Map<String,String> params  = new HashMap<String,String>();
			params.put("inbh","1080");
			
			i =(String) dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.selectObject("inPutzbc.queryFileNumer",params);
			logger.error("线程--接口" + dataParserConfig.getId() +"执行成功记录数:"+i);		
			
			
			k =(String) dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.selectObject("inPutzbc.queryFiledFileNumer",params);
			logger.error("线程--接口" + dataParserConfig.getId() +"执行错误记录数:"+k);	
			
			lastffn =(String) dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.selectObject("inPutzbc.queryFileFiledName",params);
			logger.error("线程--接口" + dataParserConfig.getId() +"最后失败执行文件名:"+lastffn);	
			
			lastsfn =(String) dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
			.selectObject("inPutzbc.queryFileName",params);
			logger.error("线程--接口" + dataParserConfig.getId() +"最后成功执行文件名:"+lastsfn);	
			
			
			if(i.equals("0")&&k.equals("0")){//初次运行
				dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				.execute("inPutzbc.updateGongyxhd");
			}else{
				if(!k.equals("0")){//有错误的运行记录		
				     if(lastsfn.contains("ipds03")){
	                     dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						   .execute("inPutzbc.updateGongyxhd");	
					  }
				     if(lastffn.contains("ipds01")){
	                     dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						   .execute("inPutzbc.updateGongyxhd");	
					  }	
			    }else{//无错误的运行记录,多次运行
				     if(lastsfn.contains("ipds03")){
	                     dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						   .execute("inPutzbc.updateGongyxhd");	
						}	
				}
		   }
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + dataParserConfig.getId() +"更新ckx_gongyxhd表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"更新ckx_gongyxhd表时报错"+e.getMessage());
		}
	}

	/**
	 * 行解析之前处理方法
	 * 处理对消耗点小于5位的判断
	 * hzg 2016.4.4
	 */
	@Override
	public boolean beforeRecord(String line, String fileName, int lineNum){
		boolean result = true;
		String lineStr=line.toString();    
        if(lineStr.indexOf("PDS—ATHENA")!=-1||lineStr.indexOf("BEGIN==>")!=-1){
        	result = false;
        }
        else{                 
        	result =  true;
        }
        if(lineStr.substring(0, 9).trim().length()<5){
        	result = false;
        }
        return result;

	}

}
