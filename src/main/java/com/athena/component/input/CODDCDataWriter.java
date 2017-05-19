package com.athena.component.input;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.SpaceFinal;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.TableRecord;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.CommonUtil;
import com.athena.util.exception.ServiceException;

public class CODDCDataWriter extends TxtWriterDBTask {
	protected static Logger logger = Logger.getLogger(CODDCDataWriter.class);	//定义日志方法 
	//private  String datasourceId = "";
	 
	public CODDCDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}

	/**
	 * 解析后处理方法
	 * hzg 2013-4-2
	 */
	@Override
	public boolean beforeRecord(Record record) {
		try {
				//业务处理 
				logger.info("线程--接口" + interfaceId + "文件" + fileName + "解析第" + record.getLineNum() + "行数据  进入beforeRecord");
				String CODDC = record.getString("coddc"); 
				String lingj = record.getString("lingj");  //零件号
				String xiaohd = record.getString("xiaohd"); //消耗点
				String sydw = record.getString("danw");  //单位
				double  d_xhdxs= 0;
				String xhdxs = record.getString("xhdxs"); //消耗点系数
				String filename_A="ath4ipds03.txt";//新能源产线
				String temp="";
				
				//logger.error("线程--接口" + dataParserConfig.getId() +"fileName:"+fileName);				
				
				if(fileName.equals(filename_A)){//如果当前文件时新能源产线的文件则进行特殊处理	
					//检查是否重复CODDC并且替换为in_clv_coddc_t中的coddc
					temp = CODDC.replaceFirst("0","A");	//替换第一位为A	
					CODDC =temp;
				}

				
				try {
					  d_xhdxs=Double.parseDouble(xhdxs);
				} catch (NumberFormatException e) {
					logger.error("消耗点系数为空，"+e.getMessage());
					 throw new WarmBusinessException("消耗点系数为空"+e.getMessage());
				}
				String EMON = record.getParam().get("emon").toString(); //EMON商业化时间
				//拼接用户中心 csy 20160421
				String usercenter = CommonUtil.getUsercenter(xiaohd.substring(0, 1)); //用户中心
				String[] emon=EMON.split(",");
				//自定义的插入方法标识 hzg 2013-4-2 
				for(int y=0;y<emon.length;y++){
					String stardateStr="";
					String enddateStr="";
					String[] dataStr=null;
					if(emon[y].indexOf("-")!=-1){
						//如果时间为20120501-20120502
						dataStr=emon[y].split("-");
						stardateStr=DateTimeUtil.DateStr(dataStr[0]);
						enddateStr=DateTimeUtil.DateStr(dataStr[1]);
						//logger.info("线程--接口" + interfaceId + "文件" + fileName + "解析第" + record.getLineNum() + "行数据   解析日期" + EMON);
						AddTable(usercenter,CODDC,lingj,xiaohd,sydw,d_xhdxs,stardateStr,enddateStr);
						dataParserConfig.getInsertCount();
					}else{
						String em = DateTimeUtil.DateStr(emon[y].toString());
						//logger.info("线程--接口" + interfaceId + "文件" + fileName + "解析第" + record.getLineNum() + "行数据   解析日期" + EMON);
						AddTable(usercenter,CODDC,lingj,xiaohd,sydw,d_xhdxs,em, em);
						dataParserConfig.getInsertCount();
					}
					//logger.info("线程--接口" + interfaceId + "文件" + fileName + "解析第" + record.getLineNum() + "行数据   解析完成");
				}
		}catch(RuntimeException e){
			dataParserConfig.getErrorCount();
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据错误  "+record.getString("lingj") + e.getMessage());
			/* 2015-9-14 日志过大 生产环境空间不足 
			throw new WarmBusinessException("插入CODDC-消耗点零件错误！零件号:"+record.getString("lingj")
					+"消耗点:"+record.getString("xiaohd")+"消耗系数:"+record.getString("xhdxs")+e.getMessage());
			*/
			
		}
		//logger.info("线程--接口" + interfaceId + "文件" + fileName + "解析第" + record.getLineNum() + "行数据  跳出beforeRecord");
		return true;
	}

	@Override
	public void exec(TableRecord tableReocrd, Record record) throws SQLException {
		
	}
	
	/**
	 * 新增CODDC-消耗点零件表数据
	 * @param dataStr
	 * @param xhdxs
	 * @throws Exception 
	 * @throws SQLException 
	 */
	public void AddTable(String usercenter,String CODDC,String lingj,String xiaohd,String sydw,double d_xhdxs,
			String dataStartStr,String dataEndStr){
		//String  shengcx = GetChanx(usercenter,xiaohd); //得到生产线号 2014.1.23 del
		/*Map<String,String> params = new HashMap<String,String>();
		params.put("usercenter", strNull(usercenter)); 
		params.put("coddc", strNull(CODDC)); 
		params.put("lingj", strNull(lingj)); 
		params.put("xiaohd", strNull(xiaohd)); 
		params.put("sydw", strNull(sydw)); 
		params.put("d_xhdxs", String.valueOf(d_xhdxs)); 
		params.put("shengcxbh", strNull(shengcx)); 
		params.put("dataStartStr", strNull(dataStartStr)); 
		params.put("dataEndStr", strNull(dataEndStr)); */
		StringBuffer sqlbuf=new StringBuffer();
		sqlbuf.append("insert into "+SpaceFinal.spacename_ddbh+".in_coddc (USERCENTER,CODDC,LINGJ,XIAOHD,DANW,XIAOHXS,SHENGCX,ECOMQSSJ,ECOMJSSJ,CHULZT,CREATOR,CREATE_TIME) values(");
		sqlbuf.append("'"+strNull(usercenter)+"',");
		sqlbuf.append("'"+strNull(CODDC)+"',");
		sqlbuf.append("'"+strNull(lingj)+"',");
		sqlbuf.append("'"+strNull(xiaohd)+"',");
		sqlbuf.append("'"+strNull(sydw)+"',");
		sqlbuf.append(""+d_xhdxs+",");
		sqlbuf.append("(select t.shengcxbh from "+SpaceFinal.spacename_ddbh+".ckx_fenpq t ");
		sqlbuf.append("where usercenter = '"+strNull(usercenter)+"' and fenpqh = substr('"+strNull(xiaohd)+"', 1, 5)),");
		sqlbuf.append("to_date('"+strNull(dataStartStr)+"','yyyy-MM-dd'),");
		sqlbuf.append("to_date('"+strNull(dataEndStr)+"','yyyy-MM-dd'),"); //add by pan.rui
		sqlbuf.append("'0',");
		sqlbuf.append("'3030',");
		sqlbuf.append("sysdate)");
		PreparedStatement ps =  null;
		try {
			ps = connection.prepareStatement(sqlbuf.toString());
			ps.executeUpdate();
			//ps.close();
			/*dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.insertCoddc",params);*/
		}  catch (SQLException e) {
			logger.error("线程--接口" + dataParserConfig.getId() +"新增CODDC-消耗点零件表数据时报错，零件:"+lingj+"消耗点:"+xiaohd+"消耗系数:"+d_xhdxs+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"新增CODDC-消耗点零件表数据时报错，零件:"+lingj+"消耗点:"+xiaohd+"消耗系数:"+d_xhdxs+e.getMessage());
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
	 * 在参考系分配区表里取出生产线号
	 * @return
	 */
	/*public String GetChanx(String usercenter,String xiaohd){
		String shengcxbh="";
		StringBuffer strbuf=new StringBuffer();
		strbuf.append("select shengcxbh from "+SpaceFinal.spacename_ddbh+".ckx_fenpq ");
		strbuf.append("where usercenter = '"+strNull(usercenter)+"' and fenpqh = substr('"+strNull(xiaohd)+"', 1, 5)");
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = connection.prepareStatement(strbuf.toString());
			rs = ps.executeQuery();
			while(rs.next()){
				shengcxbh = rs.getString("shengcxbh");
			}
			//ps.close();
			//rs.close();
		} catch (SQLException e) {
			logger.error("线程--接口" + dataParserConfig.getId() +"查询参考系分配区表里取出生产线号表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询参考系分配区表里取出生产线号表时报错"+e.getMessage());
		}finally{
			try {
				if(ps != null){
					ps.close();	
				}
				if(rs != null){
					rs.close();	
				}
			} catch (SQLException e) {
				logger.error("线程--接口" + dataParserConfig.getId() +"关闭连接错误"+e.getMessage(),e);
			}
		}

		return shengcxbh;
	}*/

	/**
	 * 空串处理
	 * 
	 * @param obj
	 *            对象
	 * @return 处理后字符串
	 * @author GJ
	 * @date 2011-10-26
	 */
	private String strNull(Object obj) {// 对象为空返回空串,不为空toString
		return obj == null ? "" : obj.toString();
	}
	
	
	/**
	 * 检查in_coddc是否有重复Coddc
	 * @param lcdv
	 * @return
	 */
	public boolean CheckDobuleCoddc(String CODDC){
			String k = "";//个数 
			try{
				Map<String,String> params = new HashMap<String,String>();
				params.put("CODDC", strNull(CODDC));
				
				k =(String)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				.selectObject("inPutddbh.checkDobuleCoddc", params);
			}catch(Exception e){
				logger.error("线程--接口" + dataParserConfig.getId() +"查询in_coddc表该coddc是否已经存在"+e.getMessage());
				throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询in_coddc表该coddc是否已经存在"+e.getMessage());
			}			
			//logger.error("线程--接口" + dataParserConfig.getId() +"k："+k);			
			
			if(!k.equals("0")){
				return false;	//已经存在该coddc
			}else{
				return true;   //不存在该coddc
			}
			
	}
	
	/**
	 * 查找in_clv_coddc_t该重复Coddc的sjCoddc
	 * @param ycoddc
	 * @return
	 */
	public String FindSjCoddc(String ycoddc){
			String sjcoddc = "";//实际coddc 
			try{
				Map<String,String> params = new HashMap<String,String>();
				params.put("ycoddc", strNull(ycoddc));
				sjcoddc =(String)dataParserConfig.getBaseDao().getSdcDataSource(dataParserConfig.getWriterConfig().getDatasourceId())
				.selectObject("inPutddbh.findsjCoddc", params);
			}catch(Exception e){
				logger.error("线程--接口" + dataParserConfig.getId() +"查询in_cldv_coddc_t表该对应的sjCoddc"+e.getMessage());
				throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询in_cldv_coddc_t表该sjCoddc"+e.getMessage());
			}			
			//logger.error("线程--接口" + dataParserConfig.getId() +"找到sjcoddc为："+sjcoddc);			
			return sjcoddc; 			
	}

}
