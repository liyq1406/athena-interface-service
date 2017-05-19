package com.athena.component.input;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
 
import com.athena.component.exchange.Record;
import com.athena.component.exchange.SpaceFinal;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;

/**
 * 3050 九天排产计划(商业化的时间)
 * @author GJ
 *
 */
public class ClddxxDbDataWriter extends TxtWriterDBTask{
	protected static Logger logger = Logger.getLogger(ClddxxDbDataWriter.class);	//定义日志方法
	protected List<Map<String,String>> shangXJSList = new ArrayList<Map<String,String>>();
	protected Record jpRecord = new Record();
	protected String kanBanSJ = null; 
	protected int buZ = 0; //步长
	public ClddxxDbDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}
	
	/**
	 * 日期格式化
	 * @param datestr
	 * @return Date
	 */
	public Date dateFormatString(String datestr) { 
		String newStrDate = "";
		Date date = null;
		if(null!=datestr&&!"".equals(datestr)){
			String rq = datestr.replace(".", "");
			try {
				newStrDate = DateUtil.StringFormatddMMYYYY(rq,"ddMMyyyy");
				if(newStrDate.contains("9999")){
					newStrDate = newStrDate.replace("9999", "2099");
				}
				 date = DateUtil.stringToDateYMD(newStrDate);
			} catch (ParseException e) {
				logger.error("线程--接口" + interfaceId +"预计进焊装日期格式化转换异常  " + e.getMessage());
			}
		}
		return date;
	}

	/**
	 * 行解析之后处理方法
	 * @param record 行数据集合
	 * @author GJ
	 */
	@Override
	public boolean beforeRecord(Record record) {
		try{
			String yjjzlsj=record.getString("yjjzlsj");//获得预计进总量时间
			if(StringUtils.isNotEmpty(yjjzlsj)){
				Date yjjzlsj3 = this.dateFormatString(yjjzlsj);//预计进总量时间格式化
				record.put("yjjzlsj", yjjzlsj3);//存入集合
			}
			String yjjhzrq=record.getString("yjjhzrq");//获得预计进焊装日期
			if(StringUtils.isNotEmpty(yjjhzrq)){
				Date yjjhzrq3 = this.dateFormatString(yjjhzrq);//预计进焊装日期格式化
				record.put("yjjhzrq", yjjhzrq3);//存入集合
			}
			String yjsyhsj=record.getString("yjsyhsj");//获得预计商业化时间
			if(StringUtils.isNotEmpty(yjsyhsj)){
				Date yjsyhsj3 = this.dateFormatString(yjsyhsj);//预计商业化时间格式化
				record.put("yjsyhsj", yjsyhsj3);//存入集合
			} 
			//上线顺序号
			int t_Sxsxh = 0;
			String sxsxh = strNull(record.getString("sxsxh"));
			if(StringUtils.isEmpty(sxsxh)){
				sxsxh = "0";
			}
			t_Sxsxh = Integer.parseInt(sxsxh);
			record.put("sxsxh",t_Sxsxh);
		}catch(Exception e){
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		String usercenter = record.getString("usercenter");
		String lcdv24 = record.getString("lcdv24").substring(0,6);
		String scxh = record.getString("scxh");
		String hanzscx = GetHanBN(usercenter,lcdv24,scxh);
		
		/********************************↓由程思远于2015-12-16添加*********************************/
		//获取lcdv1400
		String lcdv1400 = record.getString("lcdv");
		
//		if ("".equals(lcdv1400)) {
//			//根据当前用户中心和whof号从in_lcdv_clddxx表中查询出相应lcdv1400，并使用该lcdv1400
//			Map<String, Object> prms = new HashMap<String, Object>();
//			prms.put("USERCENTER", usercenter);
//			prms.put("WHOF", record.getString("whof"));
//			lcdv1400 = (String) dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutddbh.queryGetLcdv", prms);
//			record.put("lcdv", lcdv1400);
//		}
		
		List<String> lcdvlistP = new ArrayList<String>();
		List<String> lcdvlistG = new ArrayList<String>();
		//按五位截取，获取P或G开头
		for (int i = 0; i < Math.ceil(lcdv1400.length()/5); i++) {
			String lcdv_s = lcdv1400.substring(i*5, i*5+5);
			if (lcdv_s.startsWith("P")) {
				lcdvlistP.add(lcdv_s);
			}
			if (lcdv_s.startsWith("G")) {
				lcdvlistG.add(lcdv_s);
			}
		}
		//排序
		Collections.sort(lcdvlistP);
		Collections.sort(lcdvlistG);
		//将截取的字符串重新拼接成新的字符串
		StringBuffer lcdvbzk = new StringBuffer();
		for (int i = 0; i < lcdvlistP.size(); i++) {
			lcdvbzk.append(lcdvlistP.get(i));
		}
		for (int i = 0; i < lcdvlistG.size(); i++) {
			lcdvbzk.append(lcdvlistG.get(i));
		}
		record.put("lcdvbzk", lcdvbzk.toString());
		/********************************↑由程思远于2015-12-16添加*********************************/
		
		record.put("cj_date", new Date());
		record.put("clzt", 0);
		record.put("hanzscx", hanzscx);
		
		
		return true;
	}
	
	
	/**
	 * 得到焊装线编号
	 * @author Hezg
	 * @date 2013-3-7
	 * @param usercenter 用户中心
	 * @param lcdv LCDV
	 * @param scxh 生产线号
	 * @return String 焊装线编号
	 */
	private String GetHanBN(String usercenter,String lcdv,String scxh){
		String hanBN="";
		StringBuffer strbuf=new StringBuffer();
		strbuf.append("select t1.chejbhhz||t1.shengcxbhhz hzbh from "+SpaceFinal.spacename_ddbh+".ckx_chexpt t1 ");
		strbuf.append("where t1.usercenter = '"+strNull(usercenter)+"' ");
		strbuf.append("and t1.CHEJBHZZ = '"+strNull(usercenter)+"5' ");
		strbuf.append("and t1.lcdv = '"+strNull(lcdv)+"' ");
		strbuf.append("and t1.shengcxbhzz = '"+strNull(scxh)+"' ");
		PreparedStatement ps =  null;
		ResultSet rs = null;
		try{	
			 ps = connection.prepareStatement(strbuf.toString());
			 rs = ps.executeQuery();
			while(rs.next()){
				hanBN = rs.getString("hzbh");
			}
		}catch(Exception e){
			logger.error("线程--接口" + dataParserConfig.getId() +"查询焊装线编号报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询焊装线编号报错"+e.getMessage());
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
		}
		return hanBN;
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
	
}
