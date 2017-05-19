package com.athena.component.input;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.FileLog;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.field.DataField;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.utils.ConvertUtils;
import com.athena.util.exception.ServiceException;
import com.toft.utils.UUIDHexGenerator;

/**
 * 发货通知数据读取
 * 
 * @author WL
 * @date 2011-10-26
 * 
 */

public class FhtzReader extends TxtInputDBSerivce {
	protected static Logger logger = Logger.getLogger(FhtzReader.class);	//定义日志方法
	public String datasourceId=null;	

	/**
	 * record放置数据集合
	 */
	private Record record = null;

	/**
	 * 解析过的code
	 */
	private Record isExist = null;

	/**
	 * 代码数组
	 */
	private String[] code = null;

	/**
	 * 开始下标数组
	 */
	private int[] start = null;

	/**
	 * 结束下标数组
	 */
	private int[] end = null;

	/**
	 * B04A001中间变量
	 */
	private Object temp = "";
	private Object tempBz = "";
	private Object tempUasl = "";

	private int fy_xuh = 0; //发运序号

	private int bz_xuh = 0; //包装序号

	private int num = 0;//插入信息总条数

	private int ztxx_num = 0; //主体信息插入数据条数

	private int fymx_num = 0;// 发运明细插入数据条数

	private int bzxx_num = 0;//包装信息插入数据条数

	private int error_num = 0;//错误数据条数


	//private Map map=null;//定义map

	private Record lingjRecord = null;
	private Record baozRecord = null;
	 
	/**
	 * 构造函数,初始化
	 * 
	 * @param dataParserConfig
	 */
	public FhtzReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId=dataParserConfig.getWriterConfig().getDatasourceId();
		record = new Record();
		isExist = new Record();
		lingjRecord = new Record();
		baozRecord = new Record();
		// 初始化代码集合,结束下标
		code = new String[] { "AT0100", "AD0300", "AC1800", "AC0500", "AH0200",
				"AC0600", "AD0400", "AA0200","AB0300", "AA0400", "AA0900",
				"AA0100", "AB0400", "AB0500", "AA0700", "AB0200"};
		start = new int[] { 0, 2, 8, 10, 11, 12, 13, 14, 16, 18, 20, 21, 22, 23, 25, 26};
		end = new int[]   { 2, 8, 10, 11, 12, 13, 14, 16, 18, 20, 21, 22, 23, 25, 26, 27};
	}


	/**
	 * 解析之前清空表数据
	 */
	@Override
	public void before() {
		try {
			//EFI发货通知-发运主体信息表是否存在处理状态为1的数据
			String sqlT01A001 = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.selectObject("inPutzxc.queryT01A001Total");
			int i = Integer.parseInt(String.valueOf(strNull(sqlT01A001)));
			if(i>0){
				//清空EDI发货通知-包装对应信息表数据
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.ediBzxxDelete");
				//清空EDI发货通知-发运明细表数据
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.ediFymxDelete");
				//清空EDI发货通知-发运主体信息表数据
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.ediFyxxDelete");
			}
		} catch (RuntimeException e) {
			logger.error("线程--接口" + dataParserConfig.getId() +"清除EDI发货通知表报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"清除EDI发货通知表报错"+e.getMessage());
		}
	}


	/**
	 * 行记录解析前数据处理
	 * @author Hezg
	 * @date 2013-1-31
	 * @param line 行数据
	 * @param fileName 文件名称
	 * @param lineNum 行数
	 * @return  boolean 
	 */
	@Override
	protected boolean readFile(String fileName, FileInputStream fileInputStream,String encoding){
		logger.info("接口" + interfaceId + "开始读取文件" + fileName);
		String encodingStr = encoding ==null?"GBK":encoding;
		int lineNum = 1;
		long startTime = System.currentTimeMillis();	
		before();
		String FileBeginTime = DateTimeUtil.getAllCurrTime();//接口文件开始运行时间
		String FileEndTime=null;//文件运行结束时间
		InputStreamReader is = null;
    	BufferedReader bufferedReader = null;
		try {	
			is = new InputStreamReader(fileInputStream,encodingStr);
			bufferedReader = new BufferedReader(is);
			String line = null;
			while((line = bufferedReader.readLine()) != null){
				// 解析文本
				complete(lineNum, dataParserConfig, line);
				lineNum++;
			}
			// 解析完文本保存文本数据
			if (record.get("T01A001")!=null) {
				this.writeTableAndLog();
			}
			num=ztxx_num+fymx_num+bzxx_num;//插入的总记录行数
			FileEndTime=DateTimeUtil.getAllCurrTime();//文件运行结束时间
			FileLog.getInstance(datasourceId).insert_file_info(getParamsFileInfo(fileName, FileBeginTime, FileEndTime,"1"),dataParserConfig.getBaseDao());//记录文件信息日志
		}catch (Exception e) {
			FileEndTime=DateTimeUtil.getAllCurrTime();//文件运行结束时间
			FileLog.getInstance(datasourceId).insert_file_info(getParamsFileInfo(fileName, FileBeginTime, FileEndTime,"-1"),dataParserConfig.getBaseDao());//记录文件信息日志
			FileLog.getInstance(datasourceId).insert_file_ErrorInfo(getParamsErrorFileInfo(e.getMessage()),dataParserConfig.getBaseDao());//记录错误信息日志
			logger.error(e.getMessage());
			throw new ServiceException("接口" + interfaceId + "读取文件" + fileName + "错误"+e.getMessage());
		}finally{
    		try{
	    		if(null!= bufferedReader){
	    			bufferedReader.close();
				}
				if (null != is) {
					is.close();
				}
				if (null != fileInputStream) {
					fileInputStream.close();
				}
    		}catch (Exception e) {
				logger.error(e.getMessage());
			}
    	}
		logger.info("接口" + interfaceId + "结束读取文件" + fileName + " 总记录行数" + (lineNum-1));
		logger.info("文件解析完成，共解析记录"+(lineNum-1)+"条,耗时"+(System.currentTimeMillis()-startTime)+"毫秒.");
		return true;
	}


	/**
	 * file_log_info日志参数
	 * @author Hezg
	 * @date 2013-3-22
	 * @param fileName
	 * @param FileBeginTime
	 * @param FileEndTime
	 * @param state
	 * @return Map<String,String>
	 */
	private Map<String,String> getParamsFileInfo(String fileName, String FileBeginTime, String FileEndTime,String state){
		Map<String,String> params = new HashMap<String,String>();
		params.put("SID", UUIDHexGenerator.getInstance().generate());
		params.put("INBH", interfaceId);
		params.put("fileName", fileName);
		params.put("file_begintime", FileBeginTime);
		params.put("file_endtime", FileEndTime);
		params.put("insert_num", String .valueOf(num));
		params.put("update_num", "0");
		params.put("error_num", String .valueOf(error_num));
		params.put("file_satus", state);
		return  params;
	}
	
	/**
	 * errorFile_info日志参数
	 * @author Hezg
	 * @date 2013-3-22
	 * @return Map<String,String>
	 */
	private Map<String,String> getParamsErrorFileInfo(String errorMessage){
		Map<String,String> params = new HashMap<String,String>();
		params.put("EID", UUIDHexGenerator.getInstance().generate());
    	params.put("SID", "");
    	params.put("INBH", interfaceId);
    	params.put("file_errorinfo",errorMessage);
    	params.put("error_date", "");
		return  params;
	}

	/**
	 * 解析文本数据
	 * 
	 * @param rowIndex
	 *            行标
	 * @param dataPareserConfig
	 *            字段配置
	 * @param line
	 *            行数
	 * @author GJ
	 * @throws SQLException
	 * @date 2011-10-26
	 */
	private void complete(int rowIndex, DataParserConfig dataPareserConfig,
			String line) throws SQLException {
		// 文本头代码
		String id = line.toString().substring(0, 6).trim();
		if(StringUtils.isNotEmpty(id)){
			for (int j = 0; j < code.length; j++) {
				if (id.equals(code[j])) {// 判断代码,转换数据
					parse(id, dataPareserConfig, line, start[j], end[j]);
				}
			}
		}
		// 如果不是第一行且开头是SERV表示数据解析完毕,保存该数据
		if (rowIndex > 1 && id.equals("SERV")) {
			this.writeTableAndLog();
		}
	}

	/**
	 * 查询in_edi_fhtz_fyxx表交付单号
	 * @author 贺志国
	 * @date 2013-1-18
	 * @param jiaofdh 交付单号
	 * @return String 交付单号
	 * @throws SQLException 异常
	 */
	private String  queryJfdhOfFyxx(String jiaofdh){
		String strJfd = "";
		try{
			strJfd = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.selectObject("inPutzxc.queryJfdhOfFyxx",strNull(jiaofdh));
		}catch (RuntimeException e) {
			logger.error("线程--接口" + dataParserConfig.getId() +"查询in_edi_fhtz_fyxx表交付单号报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询in_edi_fhtz_fyxx表交付单号报错"+e.getMessage());
		}
		return strNull(strJfd);
	}


	/**
	 * 写LOG日志
	 * @author 贺志国
	 * @date 2013-1-18
	 */
	private void writeLog(){

		StringBuilder strbuf = new StringBuilder();
		strbuf.append("交付单号已存在，交付单号="+strNull(record.getValue().get("T01A001")));
		strbuf.append(",usercenter='"+ strNull(record.getValue().get("H02AB01")).substring(0, 2)+"',");
		strbuf.append("T01B001='"+strNull(DateTimeUtil.DateFormat_Fhtz(record.getValue().get("T01B001").toString()))+"',");
		strbuf.append("D03B001='"+strNull(record.getValue().get("D03B001"))+"',");
		strbuf.append("D03D002='"+strNull(record.getValue().get("D03D002"))+"',");
		strbuf.append("D03FA01='"+record.get("D03FA01")+"',");
		strbuf.append("D03FA02='"+strNull(record.getValue().get("D03FA02"))+"',");
		strbuf.append("D03FA05='"+strNull(record.getValue().get("D03FA05"))+"',");
		strbuf.append("D03FD03='"+strNull(DateTimeUtil.DateFormat_Fhtz(record.getValue().get("D03FD03").toString()))+"',");
		strbuf.append("C18B005='"+strNull(record.getValue().get("C18B005"))+"',");
		strbuf.append("C05AB01='"+strNull(record.getValue().get("C05AB01"))+"',");
		strbuf.append("D04A001='"+strNull(record.getValue().get("D04A001"))+"',");
		logger.info(strbuf.toString());

		//记录解析完毕,清空缓存
		isExist = new Record();
		lingjRecord = new Record();
		baozRecord = new Record();
		temp = "";
		tempBz = "";
		fy_xuh = 0;
		bz_xuh = 0;
	}


	/**
	 * 提取方法，如果交付单号相同则写日志，否则写DB
	 * @author 贺志国
	 * @date 2013-1-18
	 * @throws SQLException
	 */
	private void writeTableAndLog()throws SQLException{
		//查询主表in_edi_fhtz_fyxx交付单号，看是否存在相同的交付单号
		String jiaofdh = queryJfdhOfFyxx(record.get("T01A001").toString());
		if(strNull(record.getString("T01A001")).equals(jiaofdh)){//文本中的交付单号和数据库中的交付单号相同
			this.writeLog(); //重复数据，写Log日志
		}else{
			insert();//执行插入数据
			update();//更新包装对应信息表
		}
	}

	/**
	 * 数据转换方法
	 * 
	 * @param dataPareserConfig
	 *            数据配置
	 * @param record
	 *            数据集合
	 * @param line
	 *            行数据
	 * @param start
	 *            开始下标
	 * @param end
	 *            结束下标
	 * @author GJ
	 * @date 2011-10-26
	 */
	private void parse(String id, DataParserConfig dataPareserConfig,
			String line, int start, int end) {
		Record info = new Record();
		try{
			if (id.equals("AA0200")) {
				fy_xuh++;
			}
			if (id.equals("AB0500")) {
				bz_xuh++;
			}
			// 获取配置字段集合
			DataField[] fields = dataPareserConfig.getDataFields();
			// 循环遍历配置字段集合
			for (; start < end; start++) {
				DataField field = fields[start];// 字段信息
				int s = field.getStart();// 开始下标
				int e = s + field.getLength();// 结束下标
				String value = "";
				if (s < line.length()) {// 截取数据
					value = line.substring(s, Math.min(e, line.length()));
				}
				// 如果是可重复出现的子数据,保存数据信息
				if (id.equals("AA0200") ||id.equals("AB0200")|| id.equals("AB0400")|| id.equals("AB0500")||id.equals("AB0300")) {
					info.put(field.getWriterColumn(),ConvertUtils.convertValue(field, value.trim()));
					if (id.equals("AB0400")) {
						temp = ConvertUtils.convertValue(field, value.trim());// 缓存B04A001信息
					} else if (id.equals("AB0500")) {
						info.put("B04A001", temp);
					}
					//hzg 2012-11-20 add method field 修改原因 B05AB02值 重复存map，导致所有值相同
					if (id.equals("AB0200")) {
						tempUasl = ConvertUtils.convertValue(field, value.trim());// 缓存B05AB02信息
					} else if (id.equals("AB0500")) {
						info.put("B05AB02", tempUasl);
					}
					//判断是否为包装类型（UA）AB0300->B03AB01，B03AB02；缓存第一个值B03AB01到AB0500中
					if (id.equals("AB0300")) {
						baozRecord.put("B03AB01"+fy_xuh, line.substring(6, 11).trim()); //包装类型（UA）
						tempBz = ConvertUtils.convertValue(field, line.substring(6, 11).trim());// 缓存B03AB01信息
					} else if (id.equals("AB0500")) {
						info.put("B03AB01", tempBz);
					}
					info.put("fy_xuh", fy_xuh);
					info.put("bz_xuh", bz_xuh);
				} else {
					record.put(field.getWriterColumn(),ConvertUtils.convertValue(field, value.trim()));
				}
				//将AA0400->A04A002，A04B003；的第一个值A04A002放入lingjRecord中，（第二个值默认放入到record中）
				if(id.equals("AA0400")){//零件号
					String lingjh = line.substring(6, 16).trim();  //零件
					lingjRecord.put("A04A002"+fy_xuh, lingjh);
				}

				//将AB0300->B03AB01，B03AB02；的第二个值放入record中
				if(id.equals("AB0300")){
					String B03AB02 = line.substring(23, 26).trim();//编码责任代理行
					record.put("B03AB02", B03AB02);
				}
			}
			// 计算子数据数量
			int count = count(isExist.get(id));
			isExist.put(id, count);
			record.put(id + count, info);
		}catch(RuntimeException e){
			logger.error(e.getMessage());
		}
	}

	/**
	 * 保存数据方法
	 * 
	 * @author GJ
	 * @throws SQLException
	 * @date 2011-10-26
	 */
	private void insert() throws SQLException {
		try {
			String H02AB01 = record.getValue().get("H02AB01").toString();
			String usercenter="";
			String cangkbh="";
			if(StringUtils.isNotEmpty(H02AB01)){
				usercenter = H02AB01.substring(0, 2);
				cangkbh = H02AB01.substring(H02AB01.length()-3, H02AB01.length());  //取卸货点后三位
			}
			String C18AB01=strNull(record.getValue().get("C06AB01"));   //经测试liuyc与于鹏讨论,C18AB01销售商代码与C06AB01发送者代码值取相同值，取C06AB01的值  hzg 2012-11-22。
			//将获取的发运者代码C06AB01的值，即“ 邓白氏码”转换成运输商代码
			String gongysbh = strNull(queryGongys(usercenter,C18AB01));
			//改为取实际仓库  hzg 14.3.31
			String xiehztbh =  queryShijck(usercenter,H02AB01);
			
			//于鹏确认，如果ckx_anjmlxhd表的实际仓库为空，则按原来的规则取ckx_xiehzt取卸货站台编码  hzg 2014.4.27
			if(StringUtils.isEmpty(xiehztbh)){
				//根据用户中心和仓库编号查询参考系ckx_xiehzt取卸货站台编码   
				 xiehztbh =  queryXiehztbh(usercenter,cangkbh);
			}

			// 保存发运主体信息
			// 插入数据到EFI发货通知-发运主体信息表
			Map<String,String> ztParmas = new HashMap<String,String>();
			ztParmas.put("usercenter", usercenter);
			ztParmas.put("T01A001", strNull(record.getValue().get("T01A001")));
			ztParmas.put("T01B001", strNull(DateTimeUtil.DateFormat_Fhtz(record.getValue().get("T01B001").toString())));
			ztParmas.put("D03B001", strNull(record.getValue().get("D03B001")));
			ztParmas.put("D03D002", strNull(record.getValue().get("D03D002")));
			ztParmas.put("D03FA01", strNull(record.get("D03FA01")));
			ztParmas.put("D03FA02", strNull(record.getValue().get("D03FA02")));
			ztParmas.put("D03FA05", strNull(record.getValue().get("D03FA05")));
			ztParmas.put("D03FD03", strNull(DateTimeUtil.DateFormat_Fhtz(record.getValue().get("D03FD03").toString())));
			ztParmas.put("C18B005", strNull(record.getValue().get("C18B005")));
			ztParmas.put("C05AB01", strNull(record.getValue().get("C05AB01")));
			ztParmas.put("xiehztbh", strNull(xiehztbh));
			ztParmas.put("gongysbh", gongysbh);
			ztParmas.put("D04A001", strNull(record.getValue().get("D04A001")));
			ztParmas.put("currTime", DateTimeUtil.getAllCurrTime());
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutzxc.insertEDIFyxx",ztParmas);
			
			//于鹏提出将EDI信息写到in_kdwld表  2014.5.27  hzg
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutzxc.insertKdwld",ztParmas);
			
			ztxx_num++;

			// 保存发运明细信息,AA0200代表发运明细信息
			int count = (Integer) isExist.get("AA0200");
			for (int i = 1; i <= count; i++) {
				Record AA0200 = (Record) record.get("AA0200" + i);
				Map<String,String>  mxParmas = new HashMap<String,String>();
				mxParmas.put("fy_xuh",strNull(AA0200.getString("fy_xuh")));
				mxParmas.put("T01A001",strNull(record.getValue().get("T01A001")));
				mxParmas.put("A02D001",strNull(AA0200.getString("A02D001")));
				mxParmas.put("A02D002",strNull(AA0200.getString("A02D002")));
				mxParmas.put("B03AB01",strNull(baozRecord.getString("B03AB01"+i))); //包装类型 hzg 2012-11-4 
				mxParmas.put("B03AB02",strNull(record.getValue().get("B03AB02")));
				mxParmas.put("A04A002",strNull(lingjRecord.getString("A04A002"+i)));//零件号 hzg 2012-11-4 bug 0005263
				mxParmas.put("A04B003",strNull(record.getValue().get("A04B003")));
				mxParmas.put("A09AC01",strNull(record.getValue().get("A09AC01")));
				mxParmas.put("A01C009",strNull(record.getValue().get("A01C009")));
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.insertEDIFymx",mxParmas);
				fymx_num++;	
			}

			// 保存包装对应信息,AB0500对应包装对应信息
			int num = (Integer) isExist.get("AB0500");
			for (int i = 1; i <= num; i++) {	
				Record AB0500 = (Record) record.get("AB0500" + i);		
				String B05AD01 = AB0500.getString("B05AD01");
				Map<String,String> bzParams = new HashMap<String,String>();
				bzParams.put("fy_xuh", String.valueOf(AB0500.getString("fy_xuh")));
				bzParams.put("T01A001",strNull(record.getValue().get("T01A001")));
				bzParams.put("B04A001",strNull(AB0500.getString("B04A001")));
				bzParams.put("B05AB01",strNull(AB0500.getString("B05AB01")));
				bzParams.put("B03AB01",strNull(AB0500.getString("B03AB01")));
				bzParams.put("B05AB02",strNull(AB0500.getString("B05AB02")));
				bzParams.put("B05AD01",strNull(B05AD01)); //add hzg 2014.3.7 直接取要货令值，不进行W...03的转换
				// 如果接收到要货令数据第一位字符为'W',并且最后2位为'03',则将'W'改为'L'，'03'改为'00' ; 去掉转换规则 hzg 2014.3.7
				/*if (StringUtils.isNotEmpty(B05AD01)) {
					StringBuilder strbud = new StringBuilder(B05AD01);
					if("W".equals(strbud.substring(0, 1))&&"03".equals(strbud.substring(7,9))){
						strbud.replace(0, 1, "L");
						strbud.replace(7, 9, "00");	
						bzParams.put("B05AD01",strbud.toString());
					}else{
						bzParams.put("B05AD01",B05AD01);
					}					
				}else{
					bzParams.put("B05AD01","");
				}*/
				bzParams.put("A07A001",strNull(record.getValue().get("A07A001")));
				// 录入信息到包装对应信息表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.insertEDIBzxx",bzParams);
				bzxx_num++;
			}
		}catch (RuntimeException e){
			error_num++;
			logger.error("线程--接口" + dataParserConfig.getId() +"EDI保存数据出错！"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"EDI保存数据出错！"+e.getMessage());
		} 
	}


	/**
	 * 查询零件供应商参考系表：供应商UC的包装类型，供应商UA里UC的个数，供应商UC的容量字段
	 * @author 贺志国
	 * @date 2012-11-4
	 * @param usercenter 用户中心
	 * @param C18AB01 销售商代码
	 * @param A04A002 零件号
	 * @return Map<String,String> map
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String,String>> queryUCOfLingjgys(String usercenter,String gongysbh,String lingjbh){
		// 去零件供应商参考系表：供应商UC的包装类型，供应商UA里UC的个数，供应商UC的容量字段
		List<Map<String,String>> UCList = null;
		Map<String,String> params = new HashMap<String,String>();
		params.put("usercenter",usercenter);
		params.put("gongysbh",gongysbh);
		params.put("lingjbh",lingjbh);
		try{
			UCList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.select("inPutzxc.queryUCOfLingjgys", params);
		}catch (RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"查询零件供应商参考系表出错！"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询零件供应商参考系表出错！"+e.getMessage());
		} 
		return UCList;
	}


	/**
	 * 根据邓白氏码查询供应商表中的供承运商编号
	 * @author 贺志国
	 * @date 2012-11-22
	 * @param usercenter 用户中心
	 * @param dengbsm 邓白氏码
	 * @return String gongysbh 供供承运商编号
	 */
	public String queryGongys(String usercenter,String dengbsm){
		Map<String,String> params = new HashMap<String,String>();
		String gongysbh = "";
		params.put("usercenter",usercenter);
		params.put("dengbsm",dengbsm);
		try{
			gongysbh = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.selectObject("inPutzxc.queryGongys", params);
		}catch (RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"根据邓白氏码查询供应商表中的供承运商编号出错！"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"根据邓白氏码查询供应商表中的供承运商编号出错！"+e.getMessage());
		} 
		return gongysbh;
	}
	
	
	/**
	 * 根据用户中心和仓库编号查询参考系卸货站台表
	 * 修改：mantis9402 通过ckx_anjmlxhd将EDI下传的卸货点转换成实际仓库
	 * @author 贺志国
	 * @date 2013-1-9
	 * @update Date 2014-3-31
	 * @param usercenter 用户中心
	 * @param anjmlxhd 卸货点编号
	 * @return String shijck 实际仓库
	 */
	public String queryShijck(String usercenter,String xiaohd){
		Map<String,String> params = new HashMap<String,String>();
		String xiehzt = "";
		params.put("usercenter",usercenter);
		params.put("xiaohd",xiaohd);
		try{
			xiehzt = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.selectObject("inPutzxc.queryShijck", params);
		}catch (RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"根据用户中心和仓库编号查询参考系卸货站台表出错！"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"根据用户中心和仓库编号查询参考系卸货站台表出错！"+e.getMessage());
		} 
		return xiehzt;
	}

	/**
	 * 根据用户中心和仓库编号查询参考系卸货站台表
	 * @author 贺志国
	 * @date 2013-1-9
	 * @param usercenter 用户中心
	 * @param anjmlxhd 卸货点编号
	 * @return String shijck 实际仓库
	 */
	public String queryXiehztbh(String usercenter,String cangkbh){
		Map<String,String> params = new HashMap<String,String>();
		String xiehzt = "";
		params.put("usercenter",usercenter);
		params.put("cangkbh",cangkbh);
		try{
			xiehzt = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.selectObject("inPutzxc.queryXiehztbh", params);
		}catch (RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"根据用户中心和仓库编号查询参考系卸货站台表出错！"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"根据用户中心和仓库编号查询参考系卸货站台表出错！"+e.getMessage());
		} 
		return xiehzt;
	}

	/**
	 * 更新包装对应信息表（in_edi_fhtz_bzxx）中的UC类型，UA容量，UC容量字段
	 * 条件：序号，交付单号
	 * @author 贺志国
	 * @date 2012-11-22
	 */
	private void update() throws SQLException{
		String H02AB01 = strNull(record.getValue().get("H02AB01"));
		String usercenter=null;
		if(StringUtils.isNotEmpty(H02AB01)){
			usercenter = H02AB01.substring(0, 2);
		}
		String C18AB01=strNull(record.getValue().get("C06AB01"));   //经测试liuyc与于鹏讨论,C18AB01销售商代码与C06AB01发送者代码值取相同值，取C06AB01的值  hzg 2012-11-22。
		//将获取的发运者代码C06AB01的值，即“ 邓白氏码”转换成运输商代码
		String gongysbh = strNull(queryGongys(usercenter,C18AB01));
		try{	
			//发运明细信息个数
			int count = (Integer) isExist.get("AA0200");
			for(int i=1;i<=count;i++){
				List<Map<String,String>> ucmap = this.queryUCOfLingjgys(usercenter, gongysbh, lingjRecord.getString("A04A002"+i));
				if(!ucmap.isEmpty()){
					Map<String,String> params = new HashMap<String,String>();
					params.put("ucbzlx", strNull(ucmap.get(0).get("ucbzlx")));
					params.put("uaucgs", strNull(ucmap.get(0).get("uaucgs")));
					params.put("ucrl", strNull(ucmap.get(0).get("ucrl")));
					params.put("xuh", String.valueOf(i));
					params.put("t01a001", strNull(record.getValue().get("T01A001")));
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.execute("inPutzxc.updateEDIBzxx", params);
				}
			}
		}catch (RuntimeException e){
			error_num++;
			logger.error("线程--接口" + dataParserConfig.getId() +"更新包装对应信息表（in_edi_fhtz_bzxx）中的UC类型，UA容量，UC容量字段出错！"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"更新包装对应信息表（in_edi_fhtz_bzxx）中的UC类型，UA容量，UC容量字段出错！"+e.getMessage());
		}finally {
			//记录解析完毕,清空缓存
			isExist = new Record();
			lingjRecord = new Record();
			baozRecord = new Record();
			temp = "";
			tempBz = "";
			fy_xuh = 0;
			bz_xuh = 0;
		}


	}

	/**
	 * 就算次数
	 * 
	 * @param obj
	 *            之前的次数对象
	 * @return 次数
	 * @author GJ
	 * @date 2011-10-26
	 */
	private int count(Object obj) {
		if (null == obj){// 如果次数对象为空,则是第一条
			return 1;
		} else {
			int i = (Integer) obj;// 不为空次数++
			i++;
			return i;
		}
	}

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

}
