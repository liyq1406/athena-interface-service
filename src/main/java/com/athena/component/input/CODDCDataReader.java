package com.athena.component.input;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;


/**
 * 3030 CODDC-消耗点零件 解析数据之前的处理方法
 * @date 2013-3-7
 * @author hzg
 */
public class CODDCDataReader extends TxtInputDBSerivce{
	private  String datasourceId = "";
	protected static Logger logger = Logger.getLogger(CODDCDataReader.class);	//定义日志方法
	public CODDCDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();

	}
	
	
	/**
	 * 解析数据之前清空in_coddc表数据,检查文件完整性。
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
			
			params.put("inbh","3030");
			
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
				.execute("inPutddbh.coddcDelete");
			}else{				
				if(!k.equals("0")){//有错误的运行记录				
			     if(wjgs == 1){
				      dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
					  .execute("inPutddbh.coddcDelete");	
				 }else{
					 if(lastsfn==null || lastsfn.equals("")){//今天没有成功运行记录时 
	                       dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						   .execute("inPutddbh.coddcDelete");	
					  }	
					 //如果上次执行的文件时 ipds03，则本次清除
					 if(FILE_NAME.contains("ipds03") ){
	                       dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						   .execute("inPutddbh.coddcDelete");	
					 }
					 //如果上次执行的是ipds01,且状态是失败则清除
					 if( FILE_NAME.contains("ipds01") && FILE_SATUS.equals("-1") ){ 
	                       dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						   .execute("inPutddbh.coddcDelete");	
					  }	
				  }				
		    }else{//无错误的运行记录,多次运行
			     if(wjgs == 1){
				      dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
					  .execute("inPutddbh.coddcDelete");	
				 }else{
				     if(lastsfn.contains("ipds03")){
	                       dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
						   .execute("inPutddbh.coddcDelete");	
						}	      
				  }
			}
		  }
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + dataParserConfig.getId() +"清除in_coddc表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"清除in_coddc表时报错"+e.getMessage());
		}
	}
	
	/**
	 * 解析前处理方法
	 * hzg 2013-4-2
	 */
	@Override
    public boolean beforeRecord(String line, String fileName, int lineNum) {
		boolean result = true;
		if (lineNum==1) {// 文件第一行不导入表
			result = false;
		}
		if(line!=null&&line.toString().contains("PDS—ATHENA")){
			result = false;
		}
    	return result;
    }
	
	
	/**
	 * 接口执行完后处理
	 * 使用merge模式进行数据更新  hzg 2013-9-17 mantis:8362
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void after() {		
		String filename_A="ath4ipds03.txt";//新能源产线
		
				
		//查找相同并且需要转换的Coddc，
		List<Map<String,String>> findCoddclist =dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.findCoddclist");					
		Map<String,String> params = new HashMap<String,String>();
		
		logger.info("线程--接口" + interfaceId +"替换Coddc开始");
		if(findCoddclist!= null && findCoddclist.size()>0){	
			//logger.debug("线程--接口" + interfaceId +"findCoddclist size："+findCoddclist.size());
			
			for(int i = 0;i<findCoddclist.size();i++){
				String ycoddc="";
				String sjcoddc="";
				String temp="";
				
				Map<String,String> findCoddc = findCoddclist.get(i);//某一行
				
				String Coddc = findCoddc.get("CODDC");			
				
				//logger.info("线程--接口" + interfaceId +"Coddc ："+Coddc);
				params.put("CODDC", Coddc); //需要转换的Coddc
								
				//查找in_coddc表和in_clv_coddc_t表中ycoddc相同的sjcoddc值
				sjcoddc =(String)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				.selectObject("inPutddbh.findlsjCoddc",params);	
				
				//logger.info("线程--接口" + interfaceId +"sjcoddc："+sjcoddc);
				
				/*
				if(sjcoddc==null){			
					sjcoddc =(String)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
					.selectObject("inPutddbh.findlsjCoddc2",params);	
				}
				*/
									
				if(sjcoddc!=null){
					//logger.info("线程--接口" + interfaceId +"sjcoddc[i]："+i);
					//logger.info("线程--接口" + interfaceId +"sjcoddc："+sjcoddc);
					
					params.put("SJCODDC", sjcoddc);//通过A0011111转换后得到的sjCoddc
						
					//更新in_coddc中coddc为in_cld_coddc_t中sjcoddc
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.sjcoddcUpdate",params);	
					
					//更新in_cldv_coddc表中A+CODDC为sjCODDC
					//dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.acoddcUpdate",params);	
					
				}
			}
		}

		/*更新in_coddc中coddc替换为A的coddc为0
		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.coddcUpdateA",params);		
		*/
		
		logger.info("线程--接口" + interfaceId +"替换Coddc结束");
		
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.coddcDeletedanwNull");
			
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.coddcDeleteRowidMax");
			
			
			/*3030 删除in_coddc表上次重复的Coddc、LINGJ、XIAOHD数据0010734  xss 2015.1.6
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.deleteDoubleCoddc");
			*/		
			
			
			logger.info("线程--接口" + interfaceId +"Merge表DDBH_CODDCXHDLJ开始");
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.ddbhCoddcxhdljUpdate");
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.ddbhCoddcxhdljMerge");
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.ddbhCoddcxhdljDelete");
			logger.info("线程--接口" + interfaceId +"Merge表DDBH_CODDCXHDLJ结束");
			//消耗点变更  hzg 2014.3.19  
			//1、删除xhdbgb表中失效时间小于当前时间的记录
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.xhdbgbDelete");
			//2、更新ddbh_coddcxhdlj表xiaohd，更新记录为生效时间小于当前时间的记录
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.xhdbgbUpdate");
		}catch(RuntimeException e)
		{
			//如果零件、消耗点替换出现主键冲突则逐个进行替换，并跳过冲突的零件消耗点 hzg 2014.11.28
			if(e.getMessage().contains("ORA-00001")){ 
				List<Map<String,String>> list = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.select("inPutddbh.queryCoddcLjxhdBg");
				updateLingjXhd(list);
	        }else{
	        	logger.error("线程--接口" + dataParserConfig.getId() +"根据条件Merge表DDBH_CODDCXHDLJ数据时报错"+e.getMessage());
				throw new ServiceException("线程--接口" + dataParserConfig.getId() +"根据条件Merge表DDBH_CODDCXHDLJ表数据时报错"+e.getMessage());
	        }
		}
		
		
	/*	try{
			//清除in_coddc表单位不为空的数据  hzg 2013-7-16
			String sql="delete from "+SpaceFinal.spacename_ddbh+".in_coddc where danw is null";
            PreparedStatement ps = connection.prepareStatement(sql);
			ps.executeUpdate();
            ps.close();

			//清除in_coddc表重复数据 hzg 2013-7-16
			String sqlStr = "DELETE FROM "+SpaceFinal.spacename_ddbh+".IN_CODDC A WHERE ROWID !=(SELECT MAX(ROWID) "
			+ " FROM "+SpaceFinal.spacename_ddbh+".IN_CODDC B WHERE A.USERCENTER=B.USERCENTER AND A.CODDC=B.CODDC "
			+ " AND A.LINGJ = B.LINGJ AND A.XIAOHD = B.XIAOHD AND A.ECOMQSSJ = B.ECOMQSSJ) ";
			ps = connection.prepareStatement(sqlStr);
			ps.executeUpdate();
            ps.close();

			//将in_coddc表数据拷贝到ddbh_coddcxhdlj表中 hzg 2013-7-16
			String sqlInsert = "INSERT INTO "+SpaceFinal.spacename_ddbh+".DDBH_CODDCXHDLJ SELECT USERCENTER,CODDC,LINGJ,XIAOHD,"
			+ " SHENGCX,ECOMQSSJ,ECOMJSSJ,XIAOHXS,CHULZT,"
			+ " DANW,ZHIZLX,CREATOR,CREATE_TIME from "+SpaceFinal.spacename_ddbh+".IN_CODDC ";
			ps = connection.prepareStatement(sqlInsert);
			ps.executeUpdate();
			ps.close();
		}catch(SQLException e)
		{
			logger.error("线程--接口" + dataParserConfig.getId() +"根据条件清除in_coddc表数据时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"根据条件清除in_coddc表数据时报错"+e.getMessage());
		}*/
	}
	
	
	/**
	 * 更新零件消耗点 hzg 2014.11.27
	 * @author 贺志国
	 * @date 2014-11-27
	 * @param list
	 */
	public void updateLingjXhd(List<Map<String,String>> list){
		//2循环更新零件消耗点，如果碰到主键重复的跳过
		for(Map<String,String> map : list){
			try{
				logger.info("接口" + dataParserConfig.getId() +" 更新，零件消耗点替换：lingjbh="+map.get("LINGJ")+"  xiaohd="+map.get("XIAOHD"));
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutddbh.xhdbgbUpdateOne",map);
			}catch(RuntimeException e){
				/**针对ddbh_coddcxhdlj新老消耗点替换，如果将要替换的消耗点存在于ddbh_coddcxhdlj表，
				则会报主键冲突，过滤*/
		        	logger.error("接口" + dataParserConfig.getId() +"CODDC-零件消耗点替换主键冲突，过滤：lingjbh="+map.get("LINGJ")+"  xiaohd="+map.get("XIAOHD"));
		        	continue;
			}
			
		}
	}
	
}
