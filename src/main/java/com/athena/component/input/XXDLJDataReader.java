package com.athena.component.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 1090 零件消耗点参考系
 * @date 2013-3-6
 * @author hzg
 */
public class XXDLJDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(GongzsjZXDbDataReader.class);	//定义日志方法
	private String datasourceId = "";
	public XXDLJDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
	
	/**
	 * 行解析之前处理方法
	 */
	@Override
	public boolean beforeRecord(String line, String fileName, int lineNum){
		boolean result = true;
		String str=line.toString() ;
		if(str.indexOf("PDS")!=-1){
			result = false;
		}
		
		try {
		   if(lineNum == 1){
			//ckx_lingjxhd_in 表打标 将状态改为'0';
			String zhuangt="0";
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.updateLingjxhdin",zhuangt);
			}
		  }catch (RuntimeException e) {
			logger.error("线程--接口" + interfaceId + "解析打标错误！  " + e.getMessage());
		} 
		return result;

	}

	/**
	 * 结束之后将EDIT_TIME和GONGYBS设置 为false
	 * hzg 2013-6-3
	 */
	@Override
	public void after(){
		dataParserConfig.getDataFields()[8].setUpdate(false);
		dataParserConfig.getDataFields()[9].setUpdate(false);
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
	}
	
	@Override
	public void finishAfter(){
		try {
			//ckx_lingjxhd_in 表打标 将状态改为'1';
			String zhuangt="1";
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.updateLingjxhdin",zhuangt);
		}catch (RuntimeException e) {
			logger.error("线程--接口" + interfaceId + "修改状态错误！  " + e.getMessage());
		} 
	}
}
