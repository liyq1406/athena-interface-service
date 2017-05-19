package com.athena.component.output;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.SpaceFinal;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.exception.ServiceException;
import com.toft.core3.jdbc.CannotGetJdbcConnectionException;


/**
 * 1520 订单明细输出接口
 * @author chenlei
 * @vesion 1.0
 * @date 2012-4-25
 */
public class DdmxDataWriter extends DBOutputTxtSerivce {
	protected String userCenter =  ""; //用户中心
	protected String gongysTmp = ""; //供应商代码
	protected String lingjbh = ""; //零件号
	protected String lastH = ""; //显示DH0200
	protected String tmpLastH = ""; //为只有一行数据显示DH0200
	protected Map tmp_rowObject = new HashMap();
	protected int num = 0;
	public DdmxDataWriter(DataParserConfig dataParserConfig) {
		
	}
	
	/**
	 * 判断ckx_lingj表ANJMLXHD是否为空
	 * 目的是：如果ckx_lingj表ANJMLXHD为空，就输出一个空文件，否则输出整个文件，为了支持用户维护ckx_lingj表ANJMLXHD后可以重跑
	 * hzg 2014.5.23
	 * @throws SQLException 
	 */
	@SuppressWarnings("unchecked")
	public void outPut(ExchangerConfig write, ExchangerConfig read,OutputStreamWriter out) throws SQLException {
		try{
			int cnt =this.anjmuxhdCount();
			/**hzg 2014.5.29
			 如果第一次按件目录卸货点为空，设置了SQL为outPut.emptyOutputOf1520 ,运行完后就会一直是这个SQL
			 即使配置了按件目录卸货点，第二次运行SQL仍为outPut.emptyOutputOf1520，因此要在此处先获取原有SQL配置，最后再赋值回去*/
			String querySql = read.getSql(); 
			if(cnt>0){ //存在为空的anjmlxhd
				List<Map<String,String>> lingjList = dataParserConfig.getBaseDao().getSdcDataSource(sourceId)
				.select("outPut.queryAnjmlxhdOfckxLingjValues");
				int i = 1;
				for(Map<String,String> map : lingjList){ 
					saveYicbj("ckx_lingj表共有"+cnt+"条记录Anjmlxhd值为空，第"+(i++)+"条", map.get("LINGJBH"), map.get("USERCENTER"));
				}
				read.setSql("outPut.emptyOutputOf1520");
				super.outPut(write, read, out);
			}else{
				super.outPut(write, read, out);
			}  
			read.setSql(querySql);
		}catch (RuntimeException e){
			logger.error("线程--接口" + interfaceId +"零件按件目录查询出错"+e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId + "零件按件目录查询出错"+e.getMessage());
		} 
	}
	
	/**
	 * 查询按件目录卸货点为空的记录
	 * @author 贺志国
	 * @date 2014-5-28
	 * @return anjmuxhdCount  按件目录卸货点为空的记录个数
	 */
	public int anjmuxhdCount(){
		String cnt = (String)dataParserConfig.getBaseDao().getSdcDataSource(sourceId)
		.selectObject("outPut.queryAnjmlxhdOfckxLingjCountIsNull");
		return Integer.parseInt(cnt);
	}
	
	public void executeOutPut(OutputStreamWriter out,Map<String,Object> line) {
		Map rowObject = (Map)line;
		String usercetner = rowObject.get("usercenter").toString();
		String tmp_lingjbh = rowObject.get("lingjbh").toString();
		//add gongysdm值  hzg 2014.5.30 如果供应商不同则新生成一行SERVXX
		String gongysdm = rowObject.get("gongysdm").toString();
		num++;
		tmpLastH = rowObject.get("H").toString();  //注释掉，替换后再给值 hzg 2014.3.28
		
		//mantis:9402 ，通过ckx_lingj取卸货点发给edi hzg 14.3.28  ，不用这了，直接改SQL
		/*String DH0200 = rowObject.get("H").toString(); //DH0200 表示卸货点
		//根据用户中心和零件号获取ckx_lingj表anjmlxhd
		String ajxhd = queryXiehztbh(usercetner,tmp_lingjbh);
		//替换DH0200后面的值
		 String str =DH0200.substring(6, DH0200.length()) ;
		 String newDH0200 = str.replace(str, ajxhd);
		 while(newDH0200.length()<17){//不足17位则补足17位
			 newDH0200+=" ";
		 }
		tmpLastH = newDH0200;*/
		//增加gongysdm判断条件，供应商和用户中心的判断条件一样，如果同一用户中心下出现不同供应商则生成新的SERVXX 14.5.30
		if(userCenter.equals(usercetner)&&gongysTmp.equals(gongysdm)){
				rowObject.put("A", "");
				rowObject.put("B", "");
				rowObject.put("C", "");
				rowObject.put("D", "");
				rowObject.put("E", "");
				if(lingjbh.equals(tmp_lingjbh)){
					rowObject.put("F", "");
					rowObject.put("G", "");
					lastH = rowObject.get("H").toString();
					rowObject.put("H", "");
				} else{
					rowObject.put("H", lastH+"\r\n");
					rowObject.put("F", rowObject.get("F")+"\r\n");
					rowObject.put("G", rowObject.get("G")+"\r\n");
				}
		}else{
			//DT09000000001101 替换为DT0900+sequence sql里面1101修改此处也需要修改
			String defseq = (String)dataParserConfig.getBaseDao().getSdcDataSource(sourceId).selectObject("outPut.queryDelforDT0900Seq", "");
			String bbb = rowObject.get("B").toString();
			rowObject.put("B", bbb.replace("DT09000000001101", "DT0900"+defseq));
			if(!"".equals(lastH)){
				rowObject.put("J", lastH+"\r\n");
			}
			rowObject.put("A", rowObject.get("A")+"\r\n");
			rowObject.put("B", rowObject.get("B")+"\r\n");
			rowObject.put("C", rowObject.get("C")+"\r\n");
			rowObject.put("D", rowObject.get("D")+"\r\n");
			rowObject.put("E", rowObject.get("E")+"\r\n");
			rowObject.put("F", rowObject.get("F")+"\r\n");
			rowObject.put("G", rowObject.get("G")+"\r\n");
			rowObject.put("H", "");
		}
		userCenter = usercetner;
		gongysTmp = gongysdm;
		lingjbh = tmp_lingjbh;
		tmp_rowObject.put("DDH", rowObject.get("DDH"));
		rowObject.put("usercenter", "");
		rowObject.put("gongysdm", "");
		rowObject.put("lingjbh", "");
		rowObject.put("DDH", "");
		rowObject.put("JFYYDM", rowObject.get("JFYYDM")+"\r");
		
		super.executeOutPut(out,rowObject);
	}
	
	/**
	 * 更新订单，订单明细状态为了生成文件尾部
	 * hzg 去掉Num=1 write(tmpLastH) 14.5.23
	 * 文件最后一行必需要换行，不然EDI翻译不出来
	 * @param out
	 */
	public void fileAfter(ExchangerConfig write, ExchangerConfig read,OutputStreamWriter out) {
		try {
			out.write(lastH+"\r\n");
		} catch (IOException e) {
            logger.error("接口" + interfaceId + "IO输出异常", e);
            throw new ServiceException("接口" + interfaceId + "IO输出异常", e);
		}
	}
	
	/**
	 * 1、更新ck_yaohl表的beiz1状态为T  add hzg 2014.5.23
	 * 2、更新xqjs_dingdmx表的dingdzt为5
	 * 3、更新xqjs_dingd表的dingdzt为5,beiz为T
	 */
    public void  afterAllRecords(ExchangerConfig[] ecs){
		/*改写为事务控制
		 * baseDao.getSdcDataSource(sourceId).execute("outPut.updateYaohlOfBeiz1ToT");
		baseDao.getSdcDataSource(sourceId).execute("outPut.DdmxDataMxUpdate");
		baseDao.getSdcDataSource(sourceId).execute("outPut.DdmxDataUpdate");*/
    	//判断按件目录卸货点是否存在为空的记录，如果为空则不更新订单和要货令的状态 hzg 2014.5.28
    	int cnt =this.anjmuxhdCount();
    	if(cnt==0){ //按件目录卸货点不存在为空的记录，则更新订单要货令状态
    		this.updateYaohlDingdState();
    	}
		super.afterAllRecords(ecs);
    }
    
    
    /**
     * 将更新语句放到事务中，如果出错则回滚 ; 后期如果出现网络问题可考虑放到存储过程中(hzg 2014.5.25)
     * @author 贺志国
     * @date 2014-5-25
     */
    public void updateYaohlDingdState(){
    	Connection conn = null;
		PreparedStatement ps =  null;
		try {
			conn =  baseDao.getSdcDataSource(sourceId).getConnection();
			conn.setAutoCommit(false);
			//1、更新ck_yaohl表的beiz1状态为T  outPut.updateYaohlOfBeiz1ToT
			StringBuilder yhlBuf = new StringBuilder();
			yhlBuf.append(" update "+SpaceFinal.spacename_zbc+".ck_yaohl y set y.beiz1='T' where  y.beiz1='F' ");
			ps = conn.prepareStatement(yhlBuf.toString());
			ps.execute();
			//2、更新xqjs_dingdmx表的dingdzt为5 outPut.DdmxDataMxUpdate
			StringBuilder ddmxBuf = new StringBuilder();
			ddmxBuf.append("update "+SpaceFinal.spacename_zbc+".Xqjs_Dingdmx d set d.zhuangt = '5' ");
			ddmxBuf.append(" where d.dingdh in ( select ddh.dingdh from "+SpaceFinal.spacename_zbc+".Xqjs_Dingd ddh ");
			ddmxBuf.append(" where (ddh.dingdlx='1' or ddh.dingdlx='4') and ddh.dingdzt = '4' and ddh.beiz = 'F'  and ddh.shifyjsyhl='2')");
			ps = conn.prepareStatement(ddmxBuf.toString());
			ps.execute();
			//3、更新xqjs_dingd表的dingdzt为5,beiz为T outPut.DdmxDataUpdate
			StringBuilder ddBuf = new StringBuilder();
			ddBuf.append("update "+SpaceFinal.spacename_zbc+".Xqjs_Dingd d set d.dingdzt = '5',d.beiz = 'T', ");
			ddBuf.append("d.dingdfssj=sysdate where (d.dingdlx='1' or d.dingdlx='4') ");
			ddBuf.append("and d.dingdzt = '4' and d.beiz = 'F' and d.shifyjsyhl='2'");
			ps = conn.prepareStatement(ddBuf.toString());
			ps.execute();
			conn.commit();
			ps.close();
			conn.close();
		} catch (Exception e) {
			try {
				logger.error("接口" +interfaceId+ "1520 更新要货令订单状态出错"+e.getMessage(),e);
				conn.rollback();
				throw new ServiceException("接口" + interfaceId +"1520 更新要货令订单状态出错"+e.getMessage());
			} catch (CannotGetJdbcConnectionException e1) {
	    		logger.error("接口" +interfaceId+ "不能获得数据库连接异常"+e1);
	    		throw new ServiceException("接口" + interfaceId +"1520 更新要货令订单状态出错，不能获得数据库连接"+e.getMessage());
			} catch (SQLException e1) {
				logger.error("接口" +interfaceId+ "执行出错，事务回滚"+e.getMessage(),e);
				throw new ServiceException("接口" + interfaceId +"1520 更新要货令订单状态出错"+e.getMessage());
			} 
		}finally{
			if(ps != null){
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error("接口" + interfaceId +"1520关闭连接错误"+e.getMessage(),e);
					throw new ServiceException("接口" + interfaceId +"1520 更新要货令订单状态出错"+e.getMessage());
				}		
			}
			if(conn != null){
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("接口" + interfaceId +"1520关闭连接错误"+e.getMessage(),e);
					throw new ServiceException("接口" + interfaceId +"1520 更新要货令订单状态出错"+e.getMessage());
				}	
			}
		}
		
    }
	
    

	/**
	 * 查询零件供应商信息
	 * @author 贺志国
	 * @date 2014-3-28
	 * @param USERCENTER 用户中心
	 * @param LINGJBH 零件编号
	 * @return String  anjmlxhd 按件目录卸货点
	 */
	public String queryXiehztbh(String usercenter,String lingjbh){
		Map<String,String> params = new HashMap<String,String>();
		String xiehzt = "";
		params.put("usercenter",usercenter);
		params.put("lingjbh",lingjbh);
		try{
			xiehzt = (String)dataParserConfig.getBaseDao().getSdcDataSource(sourceId)
			.selectObject("outPut.queryAnjmlxhdOfckxLingj", params);
		}catch (RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"零件编号=>"+lingjbh+" 根据用户中心和零件编号查询参考系零件表出错！"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() + "零件编号=>"+lingjbh+" 根据用户中心和零件编号查询参考系零件表出错！"+e.getMessage());
		} 
		return xiehzt;
	}
	
	
	
	/**
	 * 保存异常报警
	 * @param cuowxxxx 错误详细信息
	 * @param lingjbh 零件编号
	 * @param usercenter 用户中心
	 */
	public void saveYicbj(String cuowxxxx,String lingjbh,String usercenter){
		logger.debug(" 保存异常报警");
		Map<String,String> params = new HashMap<String,String>();
		params.put("cuowxxxx", cuowxxxx);
		params.put("lingjbh", lingjbh);
		params.put("usercenter", usercenter);
		params.put("create_time", DateTimeUtil.getAllCurrTime());
		params.put("edit_time", DateTimeUtil.getAllCurrTime());
		dataParserConfig.getBaseDao().getSdcDataSource(sourceId)
		.execute("inPutzbc.insertYicbj", params);
	}
    
}
