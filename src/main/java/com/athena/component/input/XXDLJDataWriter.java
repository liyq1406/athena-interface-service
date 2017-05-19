package com.athena.component.input;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.athena.ckx.entity.xuqjs.Fenpq;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.SpaceFinal;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.utils.ConvertUtils;
import com.athena.util.CommonUtil;
import com.athena.util.exception.ServiceException;

/**
 * 1090 零件消耗点参考系
 * @author hzg
 *
 */
public class XXDLJDataWriter extends TxtWriterDBTask {
	protected static Logger logger = Logger.getLogger(XXDLJDataWriter.class);	//定义日志方法
	public XXDLJDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}

	/**
	 * 解析之后的操作
	 * @param record 解析后的结果集
	 */
	@Override
	public boolean beforeRecord(Record record){
		//String czm = line.toString().substring(0, 1);
		String czm = record.getParam().get("CZM").toString();
		System.out.println(czm);
		String xxd = record.getString("XIAOHDBH");
		//0010374 1090 消耗点不足9位的记录不进行处理 gswang 2014-08-05
		if(!(xxd != null && xxd.length()==9)){
			logger.info("1090零件消耗点接口，消耗点长度不为9位长度,零件号："+record.getString("LINGJBH")+"，消耗点编号:"+xxd);
			return false;
		}
		//拼接用户中心 csy 20160421
		String usercenter = CommonUtil.getUsercenter(xxd.substring(0, 1));
		record.put("USERCENTER", usercenter);
		//根据消耗点编号获取anqkcts  hzg 2013-8-15
		String anqkcts = queryAnqkctsOfShengcx(xxd,usercenter);
		record.put("ANQKCTS", anqkcts);
		
		if ("M".equals(czm)) {
			record.put("GONGYBS", "1");
			record.put("BIAOS", "2");
		} else if ("D".equals(czm)) {
			record.put("GONGYBS", "0");
			record.put("BIAOS", "2");
		} else if ("S".equals(czm) || "C".equals(czm)) {
			record.put("BIAOS", "2");
			record.put("GONGYBS", "1");
//			dataParserConfig.getDataFields()[8].setUpdate(true);
//			dataParserConfig.getDataFields()[9].setUpdate(true);

		}
		record.put("CREATOR", interfaceId) ;
		record.put("EDITOR", interfaceId) ;
		record.put("CREATE_TIME", new Date()) ;
		record.put("EDIT_TIME", new Date()) ;

		/**** 增加默认值 hzg 2012-10-21 bug:0004871 ****/
		record.put("XIANBLLKC", 0);
		record.put("YIFYHLZL", 0);
		record.put("JIAOFZL", 0);
		record.put("ZHONGZZL", 0);
		
		//pds->零件消耗点修改添加 pds生效时间及失效时间
		record.put("SHURWJM", fileName);
		record.put("ZHUANGT", "2");
		
		//pds->零件消耗点修改添加 生产线编号以及分装线号 hanwu 0012490 20160301
		Fenpq fenpq = getFenpq(usercenter, xxd.substring(0, 5));
		if(fenpq != null){
			record.put("SHENGCXBH", fenpq.getShengcxbh());
			record.put("FENZXH", fenpq.getFenzxh());
		}
		
		if(!("".equals(record.getParam().get("PDSSHENGXSJ").toString()) || "".equals(record.getParam().get("PDSSHIXSJ").toString()))){
			record.put("PDSSHENGXSJ", record.getParam().get("PDSSHENGXSJ").toString());
			record.put("PDSSHIXSJ", record.getParam().get("PDSSHIXSJ").toString());
			record.put("LINGJBH", record.getString("LINGJBH"));
			record.put("XIAOHDBH", xxd);
			lingjxhdindi(record);
		}
		return true;
	}
	
	/**
	 * 获取分配区
	 * @param usercenter	用户中心
	 * @param fenpqh		分配区号
	 * @return
	 */
	private Fenpq getFenpq(String usercenter,String fenpqh){
		Fenpq f = new Fenpq();
		f.setUsercenter(usercenter);
		f.setFenpqh(fenpqh);
		f.setBiaos("1");
		List<Fenpq> list = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("ts_ckx.getFenpq",f);
		if(0 == list.size()){
			return null;
		}
		return list.get(0);
	}
	
	/**
	 * 删除表中原有的数据然后插入
	 * @author xiah
	 * @date 2015-5-20
	 * @param usercenter 用户中心
	 * @param lingjbh   零件编号
	 * @return xiaohdbh 消耗点编号
	 */
	private void lingjxhdindi(Record record){
		try {
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.deleteLingjxhdin",record.getValue());
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.insertLingjxhdin",record.getValue());
		} catch (Exception e) {
			logger.error("线程--接口" + dataParserConfig.getId() +"操作失败"+e.getMessage(),e);
		}
	}
	
	/**
	 * 获取ckx_shengcx表的安全库存天数默认值
	 * mantis:0007074
	 * @author 贺志国
	 * @date 2013-8-15
	 * @param xxd   消耗点编号
	 * @param usercenter   用户中心
	 * @return anqkcts安全库存天数
	 */
	private String queryAnqkctsOfShengcx(String xxd,String usercenter){
		Map<String,String> params = new HashMap<String,String>();
		String anqkcts = "";
		params.put("usercenter",usercenter);
		params.put("fenpqh",xxd.substring(0, 5));
		/*StringBuilder buf = new StringBuilder();
		buf.append("select s.anqkctsmrz from "+SpaceFinal.spacename_zbc+".ckx_shengcx s  where s.usercenter='").append(usercenter);
		buf.append("' and s.shengcxbh= ");
		buf.append("(select f.shengcxbh from "+SpaceFinal.spacename_zbc+".ckx_fenpq f where f.usercenter='").append(usercenter);
		buf.append("' and  f.fenpqh='").append(xxd.substring(0, 5)).append("')");
		PreparedStatement ps = null;
		ResultSet rs  = null;
		try {
				ps = connection.prepareStatement(buf.toString());
				rs  = ps.executeQuery();
			while(rs.next()){
				anqkcts = rs.getString("anqkctsmrz");
			}
		} catch (SQLException e) {
			logger.error("线程--接口" + dataParserConfig.getId() +"查询ckx_shengcx的安全库存天数报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询ckx_shengcx的安全库存天数报错"+e.getMessage());
		}finally{
			try{
				if(ps !=null){
					ps.close();
				}
				if(rs !=null){
					rs.close();
				}
			}catch(SQLException e){
				logger.error("线程--接口" + dataParserConfig.getId() +"关闭连接错误"+e.getMessage(),e);
			}
		}*/
		try {
			anqkcts = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutzbc.queryAqkctsOfFenpq",params);
		}catch (Exception e) {
			logger.error("线程--接口" + dataParserConfig.getId() +"查询ckx_shengcx的安全库存天数报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询ckx_shengcx的安全库存天数报错"+e.getMessage());
		}
		
		return anqkcts;
	}
}
