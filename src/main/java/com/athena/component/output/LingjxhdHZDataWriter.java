package com.athena.component.output;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.athena.component.exchange.SpaceFinal;
import com.athena.util.exception.ServiceException;
import com.toft.core3.ibatis.support.AbstractIBatisDao;
import com.toft.core3.jdbc.CannotGetJdbcConnectionException;

/**
 * 2740 汇总零件消耗点表
 * @author 王国首
 * @E-mail gswang@isoftstone.com
 * @version v1.0
 * @date 2014-4-3
 */
public class LingjxhdHZDataWriter extends DuoJieKouOutputWriter {
	public LingjxhdHZDataWriter() {
			
	}
	
	/**
	 * 2740输出先将biaos状态null改为1，将1的汇总，同时将1修改为2，然后将数据移动到_H表,最后将2的删除。
	 * @throws SQLException 
	 */
	@SuppressWarnings("unchecked")
	public void beforeExecute(AbstractIBatisDao baseDao,String sourceId,String id){
		Map<String,String> param = new HashMap<String,String>();
		param.put("LEIX", "9");
		//将要货令表达交付表中仓库的数据，表示为null的标识更新为0
		baseDao.getSdcDataSource(sourceId).execute("outPut.updateBiaosOfYaohlbdjf",param);
		//2汇总查询要货令表达交付表中的数据，根据用户中心，零件编号，物理点编号汇总，物理点是消耗点
		List<HashMap<String, Object>> yaohldbjf = baseDao.getSdcDataSource(sourceId).select("outPut.lingjxhdHzQuery",param);
		//3将汇总数据更新到零件仓库表中，然后跟新要货令表达交付表中表示为1的更新为标识为2，并且在一个事务中执行，出错则回滚。
		updateLingjckHz(baseDao,yaohldbjf ,sourceId,id);
		//4将_L表标识为2的数据插入到_h表备份。
		baseDao.getSdcDataSource(sourceId).execute("outPut.insertYaohlbdjfToH",param); 
		//5将_L表标识为2的数据删除掉。
		baseDao.getSdcDataSource(sourceId).execute("outPut.deleteYaohlbdjf",param);

	}
	
	private String strNull(Object obj) {// 对象为空返回空串,不为空toString
		return obj == null ? "" : obj.toString();
	}
	
	/**
	 * 
	 * @param baseDao
	 * @param yaohldbjf
	 * @param sourceId
	 * @param id
	 */
	public void updateLingjckHz(AbstractIBatisDao baseDao,List<HashMap<String, Object>> yaohldbjf, String sourceId,String id){
		if(yaohldbjf !=null && yaohldbjf.size()>0){
			Connection conn = null;
			PreparedStatement ps =  null;
			String nowTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
			for(HashMap<String, Object> yaohldbjfMap : yaohldbjf){
				try {
						conn =  baseDao.getSdcDataSource(sourceId).getConnection();
						conn.setAutoCommit(false);
						StringBuffer sqlbuf=new StringBuffer();
						sqlbuf.append("UPDATE "+SpaceFinal.spacename_zxc+".ckx_lingjxhd L SET L.edit_time = to_date('"+nowTime+"','yyyy-mm-dd hh24:mi:ss')"); 
						sqlbuf.append(",L.YIFYHLZL = NVL(L.YIFYHLZL,0)+"+yaohldbjfMap.get("YIFYHLZL"));
						sqlbuf.append(",L.JIAOFZL = NVL(L.JIAOFZL,0)+"+yaohldbjfMap.get("JIAOFZL"));
						sqlbuf.append(",L.ZHONGZZL = NVL(L.ZHONGZZL,0)+"+yaohldbjfMap.get("ZHONGZZL"));
						sqlbuf.append(" WHERE L.USERCENTER = '"+strNull(yaohldbjfMap.get("USERCENTER")));
						sqlbuf.append("' and L.LINGJBH = '"+strNull(yaohldbjfMap.get("LINGJBH")));
						sqlbuf.append("' and L.XIAOHDBH= '"+strNull(yaohldbjfMap.get("WULDBH"))+"'");
						ps = conn.prepareStatement(sqlbuf.toString());
						ps.execute();
						
						StringBuffer sqlbufhz=new StringBuffer();
						sqlbufhz.append("UPDATE  "+SpaceFinal.spacename_zxc+".CK_YAOHLBDJF_L L  SET L.BIAOS = '2' WHERE L.BIAOS = '1' AND L.USERCENTER =");
						sqlbufhz.append("'"+strNull(yaohldbjfMap.get("USERCENTER"))+"' and L.LINGJBH=");
						sqlbufhz.append("'"+strNull(yaohldbjfMap.get("LINGJBH"))+"' and L.WULDBH=");
						sqlbufhz.append("'"+strNull(yaohldbjfMap.get("WULDBH"))+"'");
						ps = conn.prepareStatement(sqlbufhz.toString());
						ps.execute();						
						conn.commit();
						ps.close();
						conn.close();
				} catch (Exception e) {
					try {
						logger.error("接口" + "执行出错回滚"+e);
						conn.rollback();
						throw new ServiceException("线程--接口" + id +"要货令表达交付汇总"+e.getMessage());
					} catch (CannotGetJdbcConnectionException e1) {
			    		logger.error("接口" + "执行出错"+e1);
			    		throw new ServiceException("线程--接口" + id +"要货令表达交付汇总"+e.getMessage());
					} catch (SQLException e1) {
			    		logger.error("接口" + "执行出错"+e1);
			    		throw new ServiceException("线程--接口" + id +"要货令表达交付汇总"+e.getMessage());
					}
				}finally{
					if(ps != null){
						try {
							ps.close();
						} catch (SQLException e) {
							logger.error("线程--接口" + id +"关闭连接错误"+e.getMessage(),e);
							throw new ServiceException("线程--接口" + id +"要货令表达交付汇总"+e.getMessage());
						}		
					}
					if(conn != null){
						try {
							conn.close();
						} catch (SQLException e) {
							logger.error("线程--接口" + id +"关闭连接错误"+e.getMessage(),e);
							throw new ServiceException("线程--接口" + id +"要货令表达交付汇总"+e.getMessage());
						}	
					}

		    	}
			}	
		}
	}
	
}
