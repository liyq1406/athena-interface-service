package com.athena.component.input;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.date.DateUtil;

/**
 *1040 零件参考系接口输入类
 * @author HZG
 * @date 2013-1-30
 */
public class ComponentDbDataWriter extends TxtWriterDBTask {
	protected static Logger logger = Logger.getLogger(ComponentDbDataWriter.class);	//定义日志方法 
    private String usercenterTemp = "";
    private int numb = 1;//频次
    private List<String> list = new ArrayList<String>();
    Map<String,String> dhcjStr = new HashMap<String,String>();
    Map<String,String> ucMap = new HashMap<String,String>();
    private List<Map<String,String>>  ucList  =  new ArrayList<Map<String,String>>();
    private List<Map<String,String>> dhcjList =  new ArrayList<Map<String,String>>();
	public ComponentDbDataWriter(DataParserConfig dataParserConfig,
			List<String> fieldList, List<String> updateFieldList,
			String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}

	/**
	 * 时间格式化
	 * @param time 日期
	 * @param lineNum 行号
	 * @return Date 返回日期
	 * @throws ParseException 
	 */
	public Date substringTransfer(String time,int lineNum) throws ParseException
	{
		Date datetime = null;
		if(time.length()!=0){
			datetime = DateUtil.stringToDateYMD(DateUtil.StringFormatddMMYYYY(time, "ddMMyy"));
		}else{
			logger.info("线程--接口" + interfaceId +  "第" + lineNum +" 条数据开始日期或结束日期为空");
		}
		return datetime;

	}

	/**
	 * 行解析之后处理方法
	 * @param rowIndex 行标
	 * @param record 行数据集合
	 * @author GJ
	 * @update hzg 2012-10-11  直接写业务表
	 */
	@SuppressWarnings("unchecked")
	@Override
	public  boolean beforeRecord(Record record) {
		String ljdhcj=record.getString("dinghcj");//取零件订货车间
		String zhizlx = record.getString("zhizlx");//取制造路线 
		String lingjbh = record.getString("lingjbh");
		
		//法文名转换 hzg 2014.8.20
		String strFawmc = record.getString("fawmc");
		String regEx="[^A-Za-z0-9]"; //匹配由数字和26个英文字母组成的字符串
		Pattern pat=Pattern.compile(regEx);
		Matcher mat=pat.matcher(strFawmc);
		String fawmc=mat.replaceAll(" "); //将除数字和26个英文字母外的字符转成空字符串
		record.put("fawmc", fawmc.toUpperCase());
		//中文名称转换 WY 2014.9.25  如果有英文的逗号 和 英文的单引号 则转换成中文的 逗号 和 空格
		String zwmc = record.getString("zhongwmc");
		if(null!=zwmc && zwmc.length()>0){
			zwmc = zwmc.replaceAll(",", "").replaceAll("'", "").replaceAll("\"", "").replaceAll("%", "");
			zwmc = zwmc.replace("(", "").replace(")", "").replace("<", "").replace(">", "").replace("{", "").replace("}", "")
					   .replace(":", "").replace(";", "").replace("~", "").replace("|", "");
			record.put("zhongwmc", zwmc);
		}
		String usercenter = "";
		if(ljdhcj.length()!=0){
			usercenter=ljdhcj.substring(0, 2);//取零件订货车间前2个字符为用户中心
			record.put("usercenter", usercenter);
		}
		// ------订货车间转换功能 hzg 2014.4.27 --------
		  //1、直接取出三个用户中心的订货车间集合，只查询一次
		if(numb==1){
			dhcjList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzbc.queryXitcsdyOfDinghcj");
			for(Map<String,String> dhcj : dhcjList){
				dhcjStr.put(dhcj.get("USERCENTER"), dhcj.get("BEIZ"));
			}
			ucList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzbc.queryUsercenterList");
			for(Map<String,String> ucm : ucList){
				ucMap.put(ucm.get("USERCENTER"), ucm.get("USERCENTER"));
			}
		}
		numb ++;
		
		/////对于跨中心记录的处理会新增一条零件记录插入或更新系统中，isUpdate="false"修改时，或调整字段修改插入时，需要修改此处的代码
		//////gswang 2015-03-04
		if(ljdhcj.length()>=2 && zhizlx.length()>=2){
			String ljdhcjUc = ljdhcj.substring(0, 2);
			String zhizlxUc = zhizlx.substring(0, 2);
			if(ucMap.get(ljdhcjUc)!= null && ucMap.get(ljdhcjUc).length()>0 
					&& ucMap.get(zhizlxUc)!= null && ucMap.get(zhizlxUc).length()>0 &&  !ljdhcjUc.equals(zhizlxUc)){
				Record ucRecord = new Record();
				ucRecord.put("usercenter", zhizlxUc);
				ucRecord.put("dinghcj", zhizlx);
				ucRecord.put("lingjbh", record.getString("lingjbh"));
				ucRecord.put("kaisrq", record.getString("kaisrq"));
				ucRecord.put("jiesrq", record.getString("jiesrq"));
				ucRecord.put("zhizlx", record.getString("zhizlx"));
				ucRecord.put("danw", record.getString("danw"));
				ucRecord.put("zhongwmc", record.getString("zhongwmc"));
				ucRecord.put("fawmc", record.getString("fawmc"));
				ucRecord.put("biaos", record.getString("biaos"));
				
				ucRecord.put("creator", record.getString("creator"));
				ucRecord.put("create_time", record.getString("create_time"));
				ucRecord.put("editor", record.getString("editor"));
				ucRecord.put("edit_time", record.getString("edit_time"));
				this.copyRecord(zhizlx,zhizlx,ucRecord);
			}
		}
		
		//判断用户中心是否属于哪个中心
		String ulDhcj = dhcjStr.get(usercenter);//取出用户中心对应的订货车间字符串值
		if(StringUtils.isNotEmpty(ulDhcj)){
			convertDinghcj(usercenter,lingjbh,ulDhcj,ljdhcj,record);
		}

		
		// ------制造路线转换功能--------
		//查询制造路线 hzg 2014.4.26
		String zhizlxx = this.queryZhizxl(zhizlx, usercenter);
		//制造路线转换查询无数据,保存异常报警
		if(StringUtils.isEmpty(zhizlxx)){
			saveYicbj("接口"+interfaceId+" 现制造路线查询无数据,原制造路线"+zhizlx, lingjbh, usercenter);
		}else{
			//判断是否是同一用户中心
			if(!usercenterTemp.equals(usercenter)){
				//订货路线转换零件查询
				list = new ArrayList<String>();
				list = this.queryLingjOfDinghlxzh(usercenter);
			}
			if(list.contains(lingjbh)){
				//订货路线转换，用户中心，零件编号，制造路线查询
				String dinghlx = this.queryDinghlx(usercenter, lingjbh, zhizlxx);
				if(StringUtils.isEmpty(dinghlx)){
					saveYicbj("接口"+interfaceId+" 订货路线查询无数据,现制造路线"+zhizlxx, lingjbh, usercenter);
					zhizlx = zhizlxx;
				}else{
					zhizlx = dinghlx;
				}
			}else{
				zhizlx = zhizlxx;
			}
		}
		record.put("zhizlx", zhizlx);

		usercenterTemp = usercenter;
		try {	
			//取开始日期
			String ksrq=record.getString("kaisrq");
			if(ksrq.length()!=0&&"999999".equals(ksrq)){//如果开始日期为99999将日期格式转2099-12-31
				record.put("kaisrq",DateUtil.stringToDateYMD("2099-12-31"));
			}
			else{
				record.put("kaisrq", substringTransfer(ksrq,record.getLineNum()));
			}
			//取结束日期
			String jsrq =  record.getString("jiesrq");
			if(jsrq.length()!=0&&"999999".equals(jsrq)){//如果结束日期为99999将日期格式转成2099-12-31
				record.put("jiesrq",DateUtil.stringToDateYMD("2099-12-31"));
			}
			else{
				record.put("jiesrq",substringTransfer(jsrq,record.getLineNum()));
			}
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		Map<String, String> paramslj = new HashMap<String, String>();
		paramslj.put("usercenter", record.getString("usercenter"));
		paramslj.put("lingjbh", record.getString("lingjbh"));
		paramslj.put("zhongwmc", record.getString("zhongwmc"));
		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.updateCkxlingjZhongwmc", paramslj);
		//存入创建时间和处理状态初始数据
		record.put("biaos", 2);
		record.put("creator", interfaceId) ;
		record.put("editor", interfaceId) ;
		record.put("create_time", new Date()) ;
		record.put("edit_time", new Date()) ;
		return true;
	}
	
	
	/**
	 * 查询制造路线是否存在
	 * @author 贺志国
	 * @date 2014-4-26
	 * @param zhizlx 文本读取制造路线
	 * @param usercenter 用户中心
	 * @return String 制造路线
	 */
	public String queryZhizxl(String zhizlx,String usercenter){
		logger.debug("查询制造路线");
		Map<String,String> param = new HashMap<String,String>();
		param.put("zhizlx", zhizlx);
		param.put("usercenter",usercenter);
		String zzlx = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutzbc.queryZhizxl", param);
		return zzlx;
	}
	
	/**
	 * 订货路线转换表零件查询
	 * @author 贺志国
	 * @date 2014-4-26
	 * @param usercenter 用户中心
	 * @return List<String> list用户中心下所有的订货路线转换零件集合
	 */
	@SuppressWarnings("unchecked")
	public List<String> queryLingjOfDinghlxzh(String usercenter){
		List<Map<String,String>> listDhlj = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutzbc.queryLingjOfDinghlxzh", usercenter);
		List<String> listStr = new ArrayList<String>();
		for(Map<String,String> map:listDhlj){
			listStr.add(map.get("LINGJBH"));
		}
		return listStr;
	}
	
	
	/**
	 * 查询订货路线
	 * @author 贺志国
	 * @date 2014-4-26
	 * @param usercenter 用户中心
	 * @param lingjbh 零件编号
	 * @param zhizlxx 现制造路线
	 * @return String 订货路线
	 */
	public String queryDinghlx(String usercenter,String lingjbh,String zhizlxx){
		Map<String,String> param = new HashMap<String,String>();
		param.put("usercenter",usercenter);
		param.put("lingjbh",lingjbh);
		param.put("zhizlxx", zhizlxx);
		String dinghlx = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutzbc.queryDinghlx", param);
		return dinghlx;
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
		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
		.execute("inPutzbc.insertYicbj", params);

	}
	
	
	
	/**
	 * 查询数据库是否存在相同的零件记录
	 * @author 贺志国
	 * @date 2014-4-27
	 * @param usercenter
	 * @param lingjbh
	 * @return
	 */
	public String queryLingjDinghcj(String usercenter,String lingjbh){
		Map<String,String> p = new HashMap<String,String>();
		p.put("usercenter", usercenter);
		p.put("lingjbh", lingjbh);
		String db_ljdhcj = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutzbc.queryLingjDhcj", p);
		return db_ljdhcj;
	}
	
	
	/**
	 * 订货车间转换
	 * @author 贺志国
	 * @date 2014-4-27
	 * @param usercenter 用户中心
	 * @param lingjbh 零件编号
	 * @param ulDhcj  "UW5,UW3,UW2,UW1,UW6,UW9"
	 * @param ljdhcj 文本订货车间
	 * @param record 记录集
	 */
	public void convertDinghcj(String usercenter,String lingjbh,String ulDhcj,String ljdhcj,Record record){
		String db_ljdhcj = this.queryLingjDinghcj(usercenter, lingjbh);//查询用户中心零件是否存在
		if(StringUtils.isNotEmpty(db_ljdhcj)){ //已存在，则更新
			//(1).判断文本零件是否在字符串值中
			int a = ulDhcj.indexOf(ljdhcj);
			if(a!=-1){//存在于字符串值中
				//判断文件db_ljdhcj在字符串值中出现的位置
				int b = ulDhcj.indexOf(db_ljdhcj);
				if(a<b){ //出现的位置在前面，则替换
					record.put("dinghcj", ljdhcj);
				}else if(b==-1){ //如果DB中的订货车间不存在于字符串中，则用文本零件订货车间替换
					record.put("dinghcj", ljdhcj);
				}else{
					record.put("dinghcj", db_ljdhcj);//DB订货车间位置小于文本订货车间位置，则不替换
				}
			}else{
				record.put("dinghcj", db_ljdhcj);//文本订货车间不存在于字符串中，则不替换
			}
		}
	}
	
	/**
	 * 跨用户中心记录的处理
	 * @author 王国首
	 * @date 2015-3-3
	 * @param usercenter 用户中心
	 * @param record 记录集
	 */
	public void copyRecord(String ljdhcj,String zhizlx,Record record){
		String lingjbh = record.getString("lingjbh");
		String usercenter = zhizlx.substring(0, 2);
		//判断用户中心是否属于哪个中心
		
		String ulDhcj = dhcjStr.get(usercenter);//取出用户中心对应的订货车间字符串值
		if(StringUtils.isNotEmpty(ulDhcj)){
			convertDinghcj(usercenter,lingjbh,ulDhcj,ljdhcj,record);
		}
	
		// ------制造路线转换功能--------
		//查询制造路线 hzg 2014.4.26
		String zhizlxx = this.queryZhizxl(zhizlx, usercenter);
		//制造路线转换查询无数据,保存异常报警
		if(StringUtils.isEmpty(zhizlxx)){
			saveYicbj("接口"+interfaceId+" 现制造路线查询无数据,原制造路线"+zhizlx, lingjbh, usercenter);
		}else{
			//判断是否是同一用户中心
			if(!usercenterTemp.equals(usercenter)){
				//订货路线转换零件查询
				list = new ArrayList<String>();
				list = this.queryLingjOfDinghlxzh(usercenter);
			}
			if(list.contains(lingjbh)){
				//订货路线转换，用户中心，零件编号，制造路线查询
				String dinghlx = this.queryDinghlx(usercenter, lingjbh, zhizlxx);
				if(StringUtils.isEmpty(dinghlx)){
					saveYicbj("接口"+interfaceId+" 订货路线查询无数据,现制造路线"+zhizlxx, lingjbh, usercenter);
					zhizlx = zhizlxx;
				}else{
					zhizlx = dinghlx;
				}
			}else{
				zhizlx = zhizlxx;
			}
		}
		record.put("zhizlx", zhizlx);
		usercenterTemp = usercenter;
		try {	
			//取开始日期
			String ksrq=record.getString("kaisrq");
			if(ksrq.length()!=0&&"999999".equals(ksrq)){//如果开始日期为99999将日期格式转2099-12-31
				record.put("kaisrq","2099-12-31");
			}
			else{
				record.put("kaisrq", DateUtil.dateToStringYMD(substringTransfer(ksrq,record.getLineNum())));
			}
			//取结束日期
			String jsrq =  record.getString("jiesrq");
			if(jsrq.length()!=0&&"999999".equals(jsrq)){//如果结束日期为99999将日期格式转成2099-12-31
				record.put("jiesrq","2099-12-31");
			}
			else{
				record.put("jiesrq",DateUtil.dateToStringYMD(substringTransfer(jsrq,record.getLineNum())));
			}
		} catch (ParseException e) {
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据时间转换异常  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误！"+e.getMessage());
		}
		Map<String, String> paramslj = new HashMap<String, String>();
		paramslj.put("usercenter", record.getString("usercenter"));
		paramslj.put("lingjbh", record.getString("lingjbh"));
		paramslj.put("zhongwmc", record.getString("zhongwmc"));
		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.updateCkxlingjZhongwmc", paramslj);
		//存入创建时间和处理状态初始数据
		record.put("biaos", 2);
		record.put("creator", interfaceId) ;
		record.put("editor", interfaceId) ;
		record.put("create_time", DateUtil.curDateTime()) ;
		record.put("edit_time", DateUtil.curDateTime()) ;

		Map<String,String> param = new HashMap<String,String>();
		param.put("usercenter", record.getString("usercenter"));
		param.put("dinghcj", record.getString("dinghcj"));
		param.put("lingjbh", record.getString("lingjbh"));
		param.put("kaisrq", record.getString("kaisrq"));
		param.put("jiesrq", record.getString("jiesrq"));
		param.put("zhizlx", record.getString("zhizlx"));
		param.put("danw", record.getString("danw"));
		param.put("zhongwmc", record.getString("zhongwmc"));
		param.put("fawmc", record.getString("fawmc"));
		param.put("biaos", record.getString("biaos"));
		param.put("creator", record.getString("creator"));
		param.put("create_time", record.getString("create_time"));
		param.put("editor", record.getString("editor"));
		param.put("edit_time", record.getString("edit_time"));
		int ljnum = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.updateCkxCopylingj", param);
		if(ljnum == 0 ){
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzbc.insertCkxCopylingj", param);
		}
	}

}
