package com.athena.component.input;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.SpaceFinal;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.athena.util.exception.ServiceException;

/**
 * 3040 CLDV-CODDC对应关系
 * @author hzg
 *
 */
public class CLDV_CODDCDataWriter extends TxtWriterDBTask{
		public CLDV_CODDCDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
			super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
		}


	/**
	 * 行记录解析之后  
	 * */
	@Override
	public boolean beforeRecord(Record record) {
		//业务处理 
		//logger.info("线程--接口" + interfaceId + "文件" + fileName + "解析第" + record.getLineNum() + "行数据  进入beforeRecord");		
		String lcdv=record.getString("LCDV1424");					
		String coddc=record.getString("CODDC");
		String filename_A="ath4ipds04.txt";//新能源产线
		String temp="";
		String temp2="";
		
		//logger.debug("线程--接口" + dataParserConfig.getId() +"LCDV1424:"+lcdv);
		//logger.error("线程--接口" + dataParserConfig.getId() +"coddc:"+coddc);
		//logger.error("线程--接口" + dataParserConfig.getId() +"fileName:"+fileName);
		
		record.put("cj_date", new Date());
		record.put("clzt", 0);
		//record.put("CODDC", coddc);		
		
		if(fileName.equals(filename_A)){//如果当前文件时新能源产线的文件则进行特殊处理	
			
			/*2015-8-31 xss注释
			if(CheckLcdv(lcdv,coddc)==false){//有重复lcdv1424、但是coddc不同
				String coddc_new="" ;
				coddc_new = FindLcdvCoddc(lcdv);	 //查找该重复Lcdv1424的第一个Coddc	
				AddTable(coddc,coddc_new,lcdv);//将此行的Coddc和得到的第一个Coddc插入in_clv_coddc_t表		
			}else if(CheckLcdv2(lcdv)==false){//lcdv1424不同、但是coddc相同
				temp = coddc.replaceFirst("0","A");
				record.put("CODDC", temp);	
			}else if(CheckLcdv3(lcdv,coddc)==false){//lcdv1424相同、而且 coddc相同
				temp2 = coddc.replaceFirst("0","A");
				AddTable(temp2,coddc,lcdv);//将此行重复的Coddc加A'插入in_clv_coddc_t表 A0001->00001	
			}
			*/
			
			temp = coddc.replaceFirst("0","A"); 		
			
			if(CheckLcdv(lcdv)==false){//有重复lcdv1424数据
				String coddc_new="" ;
				String sjcoddc="" ;
				
				AddTable(temp,coddc,lcdv);//替换A+CODDC和CODDC插入in_clv_coddc_t表	
				
				coddc_new = FindLcdvCoddc(lcdv);	 //查找in_cldv_coddc中重复Lcdv1424的第一个Coddc
				
				if(coddc_new!=null){					
					record.put("CODDC", coddc_new);	//新能源替换并进in_cldv_coddc表
					
					updateSjcoddc(temp,coddc_new,lcdv); 		//更新in_clv_coddc_t表中ycoddc为 sjcoddc
				}

			}else{
				record.put("CODDC", temp);	//新能源替换并进in_cldv_coddc表	
				
			}
			
			
		}
		
		return true;
	}
	
	/**
	 * 检查是否有重复lcdv1424
	 * @param lcdv
	 * @return
	 */
	public boolean CheckLcdv(String lcdv){
			String k = "";//个数 
			try{
				Map<String,String> params = new HashMap<String,String>();
				params.put("LCDV1424", strNull(lcdv));
				//params.put("CODDC", strNull(coddc));
				k =(String)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				.selectObject("inPutddbh.checkLcdv", params);
			}catch(Exception e){
				logger.error("线程--接口" + dataParserConfig.getId() +"查询in_cldv_coddc表该cldv1424是否已经存在"+e.getMessage());
				throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询in_cldv_coddc表该cldv1424是否已经存在"+e.getMessage());
			}			
			//logger.error("线程--接口" + dataParserConfig.getId() +"k："+k);			
			
			if(!k.equals("0")){
				return false;	//已经存在该lcdv1424
			}else{
				return true; //不存在该lcdv1424
			}
			
	}
	
	/**
	 * 检查是否有lcdv1424不同、但是coddc相同
	 * @param lcdv
	 * @return
	 */
	public boolean CheckLcdv2(String lcdv){
			String j = "";//个数 
			try{
				Map<String,String> params2 = new HashMap<String,String>();
				params2.put("LCDV1424", strNull(lcdv));
				j =(String)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				.selectObject("inPutddbh.checkLcdv2", params2);
			}catch(Exception e){
				logger.error("线程--接口" + dataParserConfig.getId() +"查询in_cldv_coddc表该cldv1424是否已经存在"+e.getMessage());
				throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询in_cldv_coddc表该cldv1424是否已经存在"+e.getMessage());
			}			
			logger.error("线程--接口" + dataParserConfig.getId() +"j："+j);			
			
			if(j.equals("0")){
				return false;	//不存在该lcdv1424
			}else{
				return true; //存在该lcdv1424
			}
			
	}
	
	
	/**
	 * 检查是否有lcdv1424相同、而且coddc也相同的数据
	 * @param lcdv
	 * @return
	 */
	public boolean CheckLcdv3(String lcdv,String coddc){
			String k = "";//个数 
			try{
				Map<String,String> params3 = new HashMap<String,String>();
				params3.put("LCDV1424", strNull(lcdv));
				params3.put("CODDC", strNull(coddc));
				
				k =(String)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				.selectObject("inPutddbh.checkLcdv3", params3);
			}catch(Exception e){
				logger.error("线程--接口" + dataParserConfig.getId() +"查询in_cldv_coddc表该重复数据是否已经存在"+e.getMessage());
				throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询in_cldv_coddc表该重复数据是否已经存在"+e.getMessage());
			}			
			logger.error("线程--接口" + dataParserConfig.getId() +"k："+k);			
			
			if(!k.equals("0")){
				return false;	//存在该相同lcdv1424和coddc
			}else{
				return true; //不存在相同lcdv1424和coddc
			}
			
	}
	
	
	
	
	/**
	 * 查找重复lcdv的第一个Coddc
	 * @param lcdv
	 * @return
	 */
	public String FindLcdvCoddc(String lcdv){
			String fcoddc = "";//第一个coddc 
			try{
				Map<String,String> params = new HashMap<String,String>();
				params.put("LCDV1424", strNull(lcdv));
				
				fcoddc =(String)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				.selectObject("inPutddbh.findLcdvCoddc", params);
			}catch(Exception e){
				logger.error("线程--接口" + dataParserConfig.getId() +"查询in_cldv_coddc表该重复cldv1424对应的第一个Coddc"+e.getMessage());
				throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询该重复in_cldv_coddc表该cldv1424对应的第一个Coddc"+e.getMessage());
			}			
			//logger.error("线程--接口" + dataParserConfig.getId() +"找到第一个coddc为："+fcoddc);			
			return fcoddc;
			
	}
	
	/**
	 * 查找in_cldv_coddc_t表中YCoddc的sjcoddc
	 * @param lcdv
	 * @return
	 */
	public String FindSjCoddc(String lcdv,String coddc_new){
			String sjcoddc = "";//第一个coddc 
			try{
				Map<String,String> params = new HashMap<String,String>();
				params.put("LCDV1424", strNull(lcdv));
				params.put("YCODDC", strNull(coddc_new));
				
				sjcoddc =(String)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				.selectObject("inPutddbh.findsjCoddc", params);
			}catch(Exception e){
				logger.error("线程--接口" + dataParserConfig.getId() +"查询in_cldv_coddc_t表该coddc对应的sjCoddc"+e.getMessage());
				throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询该重复in_cldv_coddc_t表该coddc对应的sjCoddc"+e.getMessage());
			}			
			logger.error("线程--接口" + dataParserConfig.getId() +"找到sjcoddc为："+sjcoddc);			
			return sjcoddc;			
	}
	
	/**
	 * 根据lcdv查找in_cldv_coddc表中coddc
	 * @param lcdv
	 * @return
	 */
	public String FindSjCoddc2(String lcdv,String coddc_new){
			String sjcoddc = "";//第一个coddc 
			try{
				Map<String,String> params = new HashMap<String,String>();
				params.put("LCDV1424", strNull(lcdv));
				params.put("YCODDC", strNull(coddc_new));
				
				sjcoddc =(String)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				.selectObject("inPutddbh.findsjCoddc2", params);
			}catch(Exception e){
				logger.error("线程--接口" + dataParserConfig.getId() +"查询in_cldv_coddc_t表该coddc对应的sjCoddc"+e.getMessage());
				throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询该重复in_cldv_coddc_t表该coddc对应的sjCoddc"+e.getMessage());
			}			
			logger.error("线程--接口" + dataParserConfig.getId() +"找到sjcoddc为："+sjcoddc);			
			return sjcoddc;			
	}
	
	
	/**
	 * 更新in_clv_coddc_t表中ycoddc为 sjcoddc
	 * @param lcdv
	 * @return
	 */
	public void updateSjcoddc(String coddc,String sjcoddc,String lcdv){
			//String coddc = "";//第一个coddc 
			
			try{
				Map<String,String> params = new HashMap<String,String>();
				params.put("LCDV1424", strNull(lcdv));
				params.put("SJCODDC", strNull(sjcoddc));
				params.put("CODDC", strNull(coddc));
				
			    dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				   .execute("inPutddbh.updateSjCoddc", params);	
			      
			}catch(Exception e){
				logger.error("线程--接口" + dataParserConfig.getId() +"查询in_cldv_coddc_t表该coddc对应的sjCoddc"+e.getMessage());
				throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询该重复in_cldv_coddc_t表该coddc对应的sjCoddc"+e.getMessage());
			}			
			//logger.debug("线程--接口" + dataParserConfig.getId() +"找到sjcoddc为："+sjcoddc);					
	}
	
	
	
		
		
	
		/**
		 * 空串处理
		 * @param obj 对象
		 * @return 处理后字符串
		 * @date 2011-10-26
		 */
		private String strNull(Object obj) {// 对象为空返回空串,不为空toString
			return obj == null ? "" : obj.toString().trim();
		}
		

		/**
		 * in_cldv_coddc_t新增重复的coddc数据
		 * @param CODDC
		 * @param LCDV1424
		 * @throws Exception 
		 * @throws SQLException 
		 */
		public void AddTable(String coddc,String coddc_new,String lcdv){
			StringBuffer sqlbuf=new StringBuffer();
			Date da = new Date();	
			SimpleDateFormat sf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");        //转换时间格式
			String c_date = sf.format(da);
			
			//logger.error("线程--接口" + dataParserConfig.getId() +"!!!!!!!coddc:"+coddc);
			//logger.error("线程--接口" + dataParserConfig.getId() +"!!!!!!!coddc_new:"+coddc_new);
			//logger.error("线程--接口" + dataParserConfig.getId() +"!!!!!!!lcdv:"+lcdv);
			
			
			sqlbuf.append("insert into "+SpaceFinal.spacename_ddbh+".in_cldv_coddc_t (ycoddc,sjcoddc,beiz,cj_date) values(");
			sqlbuf.append("'"+strNull(coddc)+"',");
			sqlbuf.append("'"+strNull(coddc_new)+"',");
			sqlbuf.append("'"+strNull(lcdv)+"',");
			sqlbuf.append("to_date('"+c_date+"','yyyy/MM/dd HH24:MI:SS'))");
			 
			PreparedStatement ps =  null;
			try {
				//logger.error("线程--接口" + dataParserConfig.getId() +"!!!!!!!:"+sqlbuf);
				//logger.error("线程--接口" + dataParserConfig.getId() +"!!!!!!!:"+c_date);
				
				ps = connection.prepareStatement(sqlbuf.toString());
				ps.executeUpdate();
			}  catch (SQLException e) {
				logger.error("线程--接口" + dataParserConfig.getId() +"插入in_cldv_coddc_t表该cldv1424上次的Coddc数据时报错:"+e.getMessage());
				throw new ServiceException("线程--接口" + dataParserConfig.getId() +"插入in_cldv_coddc_t表该Coddc上次的Coddc数据时报错:"+e.getMessage());
			} finally{
				if(ps != null){
					try {
						ps.close();
					} catch (SQLException e) {
						logger.error("线程--接口" + dataParserConfig.getId() +"关闭连接错误"+e.getMessage(),e);
					}
				}
			}
		}
		

		/**
		 * in_cldv_coddc新增相同coddc数据
		 * @param CODDC
		 * @param LCDV1424
		 * @throws Exception 
		 * @throws SQLException 
		 */
		public void AddTable2(String CODDC,String LCDV1424){
			StringBuffer sqlbuf=new StringBuffer();
			Date da = new Date();	
			SimpleDateFormat sf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");        //转换时间格式
			String c_date = sf.format(da);			
			//logger.error("线程--接口" + dataParserConfig.getId() +"!!!!!!!c_date:"+c_date);
		
			sqlbuf.append("insert into "+SpaceFinal.spacename_ddbh+".in_cldv_coddc (CODDC,LCDV1424,clzt,cj_date) values(");
			sqlbuf.append("'"+strNull(CODDC)+"',");
			sqlbuf.append("'"+strNull(LCDV1424)+"',");
			sqlbuf.append("'0',to_date('"+c_date+"','yyyy/MM/dd HH24:MI:SS'))");
			 
			PreparedStatement ps =  null;
			try {
				logger.error("线程--接口" + dataParserConfig.getId() +"!!!!!!!:"+sqlbuf);
				logger.error("线程--接口" + dataParserConfig.getId() +"!!!!!!!:"+c_date);
				
				ps = connection.prepareStatement(sqlbuf.toString());
				ps.executeUpdate();
			}  catch (SQLException e) {
				logger.error("线程--接口" + dataParserConfig.getId() +"新增in_cldv_coddc表该cldv1424上次的Coddc数据时报错:"+e.getMessage());
				throw new ServiceException("线程--接口" + dataParserConfig.getId() +"新增in_cldv_coddc表该cldv1424上次的Coddc数据时报错:"+e.getMessage());
			} finally{
				if(ps != null){
					try {
						ps.close();
					} catch (SQLException e) {
						logger.error("线程--接口" + dataParserConfig.getId() +"关闭连接错误"+e.getMessage(),e);
					}
				}
			}
		}
		
	
	
}
