package com.athena.component.output;

import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.config.ExchangerConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.date.DateUtil;

public class ZhouQddDataWriter extends DBOutputTxtSerivce{
	protected static Logger logger = Logger.getLogger(DdmxDataWriter.class);	//定义日志方法
	protected Map<String,Map<String,String>> gongyzq = null; //年周期
	protected Map<String,Map<String,String>> gongyzxu = null; //年周序
	protected String fyzqxh = ""; //
	protected boolean yearFlag = false; //
	public ZhouQddDataWriter(DataParserConfig dataParserConfig) {
	}
	
	public void executeOutPut(OutputStreamWriter out,Map<String,Object> line) {
		Map rowObject = (Map)line;
		rowObject.put("dingdhTemp", "");
		super.executeOutPut(out,rowObject);
	}
	
	public void beforeRecords(List<Map<String,String>> sourcelist) {
		Map<String,String> param = new HashMap<String,String>();
		gongyzq = paramMap(baseDao.getSdcDataSource(sourceId).select("outPut.queryGongyzq", param),"IDH");
		gongyzxu = paramMap(baseDao.getSdcDataSource(sourceId).select("outPut.queryGongyzxu", param),"IDH");
		int num = sourcelist.size()-1;
		boolean re = true;
		for(int i = num;i>=0;i--){
			Map<String,String> dingljMap = sourcelist.get(i);
			fyzqxh = (String)dingljMap.get("P0FYZQXH");
			yearFlag = false;
			dingljMap.put("SHUL", dingljMap.get("P0SL"));
			
			if(!getRiq(dingljMap,false)){
				sourcelist.remove(i);
			}
			
			Map<String,String> dingdtwo = new HashMap<String,String>();
			dingdtwo.putAll(dingljMap);
			dingdtwo.put("SHUL", dingdtwo.get("P1SL"));
			if(getRiq(dingdtwo,true)){
				sourcelist.add(dingdtwo);
			}
			
			Map<String,String> dingdthr = new HashMap<String,String>();
			dingdthr.putAll(dingljMap);
			dingdthr.put("SHUL", dingdthr.get("P2SL"));
			if(getRiq(dingdthr,true)){
				sourcelist.add(dingdthr);
			}	
			
			Map<String,String> dingdfour = new HashMap<String,String>();
			dingdfour.putAll(dingljMap);
			dingdfour.put("SHUL", dingdfour.get("P3SL"));
			if(getRiq(dingdfour,true)){
				sourcelist.add(dingdfour);
			}	
		}
		
	}
	
	public void afterAllRecords(ExchangerConfig[] ecs) {
		Map<String,String> param = new HashMap<String,String>();
		baseDao.getSdcDataSource(sourceId).execute("outPut.zhouQddDataUpdate",param);
		super.afterAllRecords(ecs);
	}
	
	public Map<String,Map<String,String>> paramMap(List<Map<String,String>> list,String name) {
		Map<String, Map<String,String>> result = new HashMap<String, Map<String,String>>();
		for(Map<String,String> map : list) {
			if (result.containsKey(map.get(name))) {
			} else {
				result.put(map.get(name), map);
			}
		}
		return result;
	}
	
	public boolean getRiq(Map<String,String> dingljMap,boolean isnext) {
		boolean result = true;
		String jiesrq = "";
		String usercenter = (String)dingljMap.get("USERCENTER");
		String chullx = (String)dingljMap.get("CHULLX");
		chullx  = chullx.substring(chullx.length()-1);
		if(isnext){
			dingljMap.put("LEIX", "P");
			if(yearFlag){
				if("P".equals(chullx)||"S".equals(chullx)){
					fyzqxh = (Integer.parseInt((fyzqxh.substring(0, 4)))+1)+"01";   
				}else if("J".equals(chullx)){
					fyzqxh = (Integer.parseInt((fyzqxh.substring(0, 4)))+1)+"-01-01";
				}
			}else{
				if("P".equals(chullx)||"S".equals(chullx)){
					fyzqxh = String.valueOf(Integer.parseInt(fyzqxh)+1);   
				}else if("J".equals(chullx)){
					fyzqxh = dateAddDays(fyzqxh,1);
				}
			}
		}
		Map<String,String> xh = null; 
		if("P".equals(chullx)){
			xh = gongyzq.get(usercenter+fyzqxh);
			if(xh == null){return false;}
			dingljMap.put("YAOHQSRQ", StringFormatToString(xh.get("MINRIQ")));
			dingljMap.put("YAOHJSRQ", StringFormatToString(xh.get("MAXRIQ")));
			jiesrq = xh.get("MAXRIQ");
		}else if("S".equals(chullx)){
			xh = gongyzxu.get(usercenter+fyzqxh);
			if(xh == null){return false;}
			dingljMap.put("YAOHQSRQ", StringFormatToString(xh.get("MINRIQ")));
			dingljMap.put("YAOHJSRQ", StringFormatToString(xh.get("MAXRIQ")));
			jiesrq = xh.get("MAXRIQ");
		}else if("J".equals(chullx)){
			dingljMap.put("YAOHQSRQ", StringFormatToString(fyzqxh));
			dingljMap.put("YAOHJSRQ", StringFormatToString(fyzqxh));
			jiesrq = fyzqxh;
		}
		dingljMap.put("JIAOFRQ", dingljMap.get("YAOHQSRQ"));
		if(jiesrq!=null && jiesrq.length()>0){
			if("12-31".equals(jiesrq.substring(jiesrq.length()-5))){
				yearFlag = true;
			}else{
				yearFlag = false;
			}
		}
		return result;
	}
	
	public static String dateAddDays(String strDate,int n){
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd",Locale.CHINA);
		Date date = new Date();
		try {
			Calendar calendar = new GregorianCalendar(); 
			calendar.setTime(format.parse(strDate)); 
			calendar.add(calendar.DATE,n);//把日期往后增加n天.整数往后推,负数往前移动 
			date=calendar.getTime();      //这个时间就是日期往后推一天的结果 
		} catch (ParseException e) {
			Logger log =  Logger.getLogger(DateUtil.class);
			log.error(e.getMessage());
		}
		return format.format(date);
	}
	
	public static String StringFormatToString(String strDate){
		DateFormat  newYMD = new SimpleDateFormat("yyyyMMdd",Locale.CHINA);
		DateFormat  oldYMD = new SimpleDateFormat("yyyy-MM-dd",Locale.CHINA);
		String newStrDate="";
		try {
			Date date = oldYMD.parse(strDate);
			newStrDate = newYMD.format(date);
		} catch (ParseException e) {
			Logger log =  Logger.getLogger(DateUtil.class);
			log.error(e.getMessage());
		}
		return newStrDate;
	}
}
