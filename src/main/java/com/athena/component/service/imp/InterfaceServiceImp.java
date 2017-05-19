package com.athena.component.service.imp;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jws.WebService;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import com.athena.component.exchange.SpaceFinal;
import com.athena.component.service.InterfaceService;
import com.athena.component.service.bean.ServiceBean;
import com.athena.util.date.DateUtil;
import com.toft.core2.dao.database.DbUtils;
import com.toft.core3.container.annotation.Component;

@WebService(endpointInterface="com.athena.component.service.InterfaceService",serviceName="/InterfaceServiceImp")
@Component
public class InterfaceServiceImp implements InterfaceService {
	protected static Logger logger = Logger.getLogger(InterfaceServiceImp.class);	//定义日志方法

	@Override
	public void setServiceBean(List<ServiceBean> list) {
		logger.info("OEDIPP开始调用接口");
		logger.info("list.size-->"+list.size());
		Connection conn=null;
		try{
			conn = DbUtils.getConnection("1");
			int i=1;
			if(null!=list){
				for (ServiceBean serviceBean : list) {
					String Dbsqdh=serviceBean.getDbsqdh();
					String Zzlx=serviceBean.getZzlx();
					String Ljh=serviceBean.getLjh();
					String Usercenter=serviceBean.getUsercenter();
					String operator = serviceBean.getOperator(); //增加操作人，外部系统给出对应供应商人 hzg 14.5.13 
					String beiz = serviceBean.getBeiz(); //增加备注 hzg 16.10.27
					if(StringUtils.isEmpty(operator)){ //OEDIPP调拨没有传操作人，给默认值
						operator = "OEDIPP";
					}
					logger.info(operator+" usercenter--->"+Usercenter+"  Diaobdh--->"+Dbsqdh
							+"  Zzlx--->"+Zzlx+"  Ljh--->"+Ljh+" beiz--->"+beiz);
					List<Map<String,String>> obj_dbsqd = this.GetDbsq(Usercenter,Dbsqdh,conn);
					if(obj_dbsqd.size()>0&&"00".equals(obj_dbsqd.get(0).get("ZHUANGT"))){ //size>0并且ZHUANGT=00的才更新  hzg 2013-10-12 
						this.UpdateDbsqd(serviceBean, conn);
					}else{
						//add by pan.rui 外部系统可能会传入10位调拨申请单
						if(obj_dbsqd.size()==0&&Dbsqdh.length() <= 8){
							this.insertDiaosq(serviceBean, conn, operator);
						}
					}
					List<Map<String,String>> obj_dbsqdmx=this.GetDbsqdmx(Usercenter,Dbsqdh,Zzlx,Ljh,conn);
					logger.info(operator+" usercenter--->"+Usercenter+"  Diaobdh--->"+Dbsqdh
							+"  Zzlx--->"+Zzlx+"  Ljh--->"+Ljh+"  obj_dbsqdmx--->"+obj_dbsqdmx);
					if(obj_dbsqdmx.size()>0&&"00".equals(obj_dbsqdmx.get(0).get("ZHUANGT"))){ //size>0并且ZHUANGT=00的才更新  hzg 2013-10-12 
						this.UpdateDbsqdmx(serviceBean, conn);
					}else{
						if(obj_dbsqdmx.size()==0&&Dbsqdh.length() <= 8){
							this.insertDiaomsqmx(serviceBean, conn, operator, i++);
						}
					}
				}
			}
		}catch(Exception e){
			logger.error("OEDIPP调用插入数据库出错："+e.getMessage());
		}finally{
			DbUtils.freeConnection(conn);
		}
		logger.info("OEDIPP结束调用接口");
	}

	/**
	 * 插入调拨申请，DB异常过滤
	 * @author 贺志国
	 * @throws SQLException 
	 * @date 2016-11-15
	 */
	private void insertDiaosq(ServiceBean bean,Connection conn,String operator)  {
		StringBuffer strbuf=new StringBuffer();
		strbuf.append("insert into "+SpaceFinal.spacename_zbc+".xqjs_diaobsq");
		strbuf.append("(usercenter,");
		strbuf.append("diaobsqdh,");
		strbuf.append("diaobsqsj,");
		strbuf.append("banc,");
		strbuf.append("huijkm,");
		strbuf.append("chengbzx,");
		strbuf.append("beiz,");
		strbuf.append("zhuangt,");
		strbuf.append("creator,");
		strbuf.append("create_time,");
		strbuf.append("editor,");
		strbuf.append("edit_time,");
		strbuf.append("active)");
		strbuf.append("values(");
		strbuf.append("'"+StrNull(bean.getUsercenter())+"',");
		strbuf.append("'"+StrNull(bean.getDbsqdh())+"',");
		strbuf.append("to_date('"+DateUtil.getCurrentDate()+"','yyyy-MM-dd'),"); //系统当前日期 14.3.25
		strbuf.append("'0001',");
		strbuf.append("'"+StrNull(bean.getKjkm())+"',");
		strbuf.append("'"+StrNull(bean.getCbzx())+"',");
		strbuf.append("'"+StrNull(bean.getBeiz())+"',");
		strbuf.append("'00',");  // 30->00 mantis:8388
		strbuf.append("'"+operator+"',");
		strbuf.append("current_timestamp,");
		strbuf.append("'"+operator+"',");
		strbuf.append("current_timestamp,");
		strbuf.append("'1')");
		try{
			DbUtils.execute(strbuf.toString(), conn);
		}catch(Throwable e){
			logger.error("OEDIPP调用插入数据库xqjs_diaobsq出错："+e.getMessage());
		}
		try{
			conn.commit();
		}catch (SQLException e) {
			logger.error("OEDIPP调用插入数据库xqjs_diaobsq出错："+e.getMessage());
		}
	}

	/**
	 *  插入调拨申请明细，DB异常过滤
	 * @author 贺志国
	 * @throws SQLException 
	 * @date 2016-11-15
	 */
	private void insertDiaomsqmx(ServiceBean bean,Connection conn,String operator,int i) {
		StringBuffer strbufmx=new StringBuffer();
		strbufmx.append("insert into "+SpaceFinal.spacename_zbc+".XQJS_DIAOBSQMX");
		strbufmx.append("(xuh,");
		strbufmx.append("usercenter,");
		strbufmx.append("diaobsqdh,");
		strbufmx.append("lux,");
		strbufmx.append("lingjbh,");
		strbufmx.append("shenbsl,");
		strbufmx.append("zhuangt,");
		strbufmx.append("yaohsj,"); //add field 14.3.25
		strbufmx.append("creator,");
		strbufmx.append("create_time,");
		strbufmx.append("editor,");
		strbufmx.append("edit_time,");
		strbufmx.append("active)");
		strbufmx.append("values(");
		strbufmx.append(""+i+",");
		strbufmx.append("'"+StrNull(bean.getUsercenter())+"',");
		strbufmx.append("'"+StrNull(bean.getDbsqdh())+"',");
		strbufmx.append("'"+StrNull(bean.getZzlx())+"',");
		strbufmx.append("'"+StrNull(bean.getLjh())+"',"); 
		strbufmx.append(""+bean.getSbsl()+",");
		strbufmx.append("'00',"); //30->00 mantis:8388
		strbufmx.append("to_date('"+StrNull(bean.getDbsq_date())+"','yyyy-MM-dd'),"); //add fieldValue 14.3.25
		strbufmx.append("'"+operator+"',");
		strbufmx.append("current_timestamp,");
		strbufmx.append("'"+operator+"',");
		strbufmx.append("current_timestamp,");
		strbufmx.append("'1')");
		try{
			DbUtils.execute(strbufmx.toString(), conn);
		}catch(Throwable e){
			logger.error("OEDIPP调用插入数据库xqjs_diaobsqmx出错："+e.getMessage());
		}
		try{
			conn.commit(); 
		}catch (SQLException e) {
			logger.error("OEDIPP调用插入数据库xqjs_diaobsqmx出错："+e.getMessage());
		}
	}


	/**
	 * 查询调拨申请单表是否有重复数据
	 * @param USERCENTER
	 * @param DIAOBSQDH
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String,String>> GetDbsq(String USERCENTER,String DIAOBSQDH,Connection conn){
		StringBuffer strbuf=new StringBuffer();
		List<Map<String,String>> list= new ArrayList<Map<String,String>>();
		try{
			strbuf.append("select usercenter,diaobsqdh,zhuangt from "+SpaceFinal.spacename_zbc+".XQJS_DIAOBSQ where ");
			strbuf.append("USERCENTER='"+StrNull(USERCENTER)+"' and ");
			strbuf.append("DIAOBSQDH='"+StrNull(DIAOBSQDH)+"'");
			list = DbUtils.select(strbuf.toString(), conn);	
		}catch(Exception e){
			logger.error(e.getMessage());
		}
		return list;
	}



	/**
	 *查询调拨申请单明细表是否有重复数据
	 * @param USERCENTER
	 * @param DIAOBSQDH
	 * @param LUX
	 * @param LINGJBH
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String,String>> GetDbsqdmx(String USERCENTER,String DIAOBSQDH,String LUX,String LINGJBH,Connection conn){
		StringBuffer strbuf=new StringBuffer();
		List<Map<String,String>> list= new ArrayList<Map<String,String>>();
		try{
			strbuf.append("select usercenter,diaobsqdh,zhuangt from "+SpaceFinal.spacename_zbc+".XQJS_DIAOBSQMX where ");
			strbuf.append("USERCENTER='"+StrNull(USERCENTER)+"' and ");
			strbuf.append("DIAOBSQDH='"+StrNull(DIAOBSQDH)+"' and ");
			strbuf.append("LUX='"+StrNull(LUX)+"' and ");
			strbuf.append("LINGJBH='"+StrNull(LINGJBH)+"' ");
			list = DbUtils.select(strbuf.toString(), conn);	
		}catch(Exception e){
			logger.error(e.getMessage());
		}
		return list;
	}


	/**
	 * 更新调拨申请单表
	 * @param servicebean
	 * modify servicebean.getDbsq_date()->DateUtil.getCurrentDate() 14.3.25
	 */
	public void UpdateDbsqd(ServiceBean servicebean,Connection conn){
		StringBuffer strbuf=new StringBuffer();
		try{
			strbuf.append("update "+SpaceFinal.spacename_zbc+".XQJS_DIAOBSQ set ");
			strbuf.append("DIAOBSQSJ=to_date('"+DateUtil.getCurrentDate()+"','yyyy-MM-dd'),");
			strbuf.append("BANC='0001',");
			strbuf.append("HUIJKM='"+StrNull(servicebean.getKjkm())+"',");
			strbuf.append("CHENGBZX='"+StrNull(servicebean.getCbzx())+"',");
			strbuf.append("BEIZ='"+StrNull(servicebean.getBeiz())+"',");
			//strbuf.append("ZHUANGT='30',");
			strbuf.append("EDIT_TIME=current_timestamp where ");
			strbuf.append("USERCENTER='"+StrNull(servicebean.getUsercenter())+"' and ");
			strbuf.append("DIAOBSQDH='"+StrNull(servicebean.getDbsqdh())+"' ");
			DbUtils.execute(strbuf.toString(),conn);
			conn.commit();
		}catch(Exception e){
			logger.error(e.getMessage());
		}
	}


	/**
	 * 更新调拨申请单表
	 * @param servicebean
	 */
	public void UpdateDbsqdmx(ServiceBean servicebean,Connection conn){
		StringBuffer strbuf=new StringBuffer();
		try{
			strbuf.append("update "+SpaceFinal.spacename_zbc+".XQJS_DIAOBSQMX set ");
			strbuf.append("SHENBSL="+servicebean.getSbsl()+",");
			//strbuf.append("ZHUANGT='30',");
			strbuf.append("EDIT_TIME=current_timestamp where ");
			strbuf.append("USERCENTER='"+StrNull(servicebean.getUsercenter())+"' and ");
			strbuf.append("LUX='"+StrNull(servicebean.getZzlx())+"' and ");
			strbuf.append("DIAOBSQDH='"+StrNull(servicebean.getDbsqdh())+"' and ");
			strbuf.append("LINGJBH='"+StrNull(servicebean.getLjh())+"'");
			DbUtils.execute(strbuf.toString(),conn);
			conn.commit();
		}catch(Exception e){
			logger.error(e.getMessage());
		}
	}



	/**
	 * 判断字符串为null时
	 * @param objStr
	 * @return
	 */
	private String StrNull(Object objStr){
		return objStr==null?"":objStr.toString();
	}

}
