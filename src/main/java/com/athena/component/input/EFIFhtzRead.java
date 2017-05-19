package com.athena.component.input;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.FileLog;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.exception.ServiceException;
import com.toft.utils.UUIDHexGenerator;


/**
 * EFI发货通知接口输入类
 * @author GJ
 * @update 代码优化   hzg  2012-10-19
 * @modify 新增ck_daohtzd_dfpv，ck_uabq_dfpv 杨志 2015-12-24
 */
public class EFIFhtzRead extends TxtInputDBSerivce {
	protected static Logger logger = Logger.getLogger(EFIFhtzRead.class);	//定义日志方法
	
	public String datasourceId=null;

	private static final int INSERT_FLAG_ALL =1;       //交付单号为空，全部插入三张表（FYZTXX,FYMX,BZDYXX）
	private static final int INSERT_FLAG_MXBZ =2;      //有交付单号，并且发运明细表不存在序号和交付单号，则插入两张表（FYMX,BZDYXX）
	private static final int INSERT_FLAG_BZ =3;        //有交付单号，并且存在序号和交付单号以及UA号，则插入一张表（BZDYXX）
	private static final int UPDATE_FLAG_BZ =4;        //更新EFI发货通知-包装对应信息表数据
	private static final String INSERT_FLAG_TBLJ ="1"; //插入数据到发货通知-同步零件信息表
	// 定义解析的字段
	private String jfdh = null;// 交付单号
	private String jf_date = null;// 交付时间
	private String fyzmzdw = null;// 发运总毛重单位
	private String fyyjdd_date = null;// 发运预计到达日期时间
	private String cysdm = null;// 承运商代码
	private String jszdm = null;// 接收者代码
	private String xzddm = null;// 卸载点代码
	private String fhrdm = null;// 发货人代码
	private String jzxh = null;// 集装箱号
	private String sftbfhtz = null;// 是否同步发货通知
	private String sfgzxlh = null;// 是否关注序列号
	private String xuh = null;// 序号
	private double d_fysl=0.0;//发运的数量
	private String jldw = null;// 发运数量计量单位
	private String bzlx = null;// 包装类型
	private String ljh = null;// 零件号
	private String ddh = null;// 订单号
	private String lygj = null;// 零件来源国家
	private String ua = null;// UA
	private String ualx = null;// UA类型
	private String uc = null;// UC
	private String uclx = null;// UC类型
	private double d_ucljsl = 0.0;// UC零件数量
	private double d_ualjsl = 0.0;// UA零件数量
	private String yhl = null;// 要货令
	private String pzh = null;// 批组号
	private String xlh = null;// 序列号
	private String ggph=null;//规格牌号
	//新增长宽高add by 潘瑞
	private String chang = null;//长
	private String kuan = null; //宽
	private String gao = null; //高
	private String gys = null;// 供应商
	private String lsh = null;// 流水号
	private int shul = 0;// 数量
	
	//xss-0011589
	private String quancbqh = null;//全程标签US号
	private String xiaohd = null;//消耗点
	//private String cangkkw = null;//订货/线边仓库库位
	private String xiaohcbh = null;//小火车编号
	private int tangc = 0;//趟次
	private String shifht = null;//是否混托（0：否，1：是）
	private String xiangd = null;//巷道
	private String gongzr = null;//工作日
	private String cangkkw = null;//订货/线边仓库库位 /定制库位
	
	private String xianbck = null;//线边仓库
	private String xianbckkw = null;//线边仓库库位
	private String changbiao = null;//厂标
	private String beiz = null;//备注
	
	
	private int num = 0;//插入总行数
	private int ztxx_num = 0;//插入主体信息行数
	private int fymx_num = 0;//插入发运明细行数
	private int bzxx_num = 0;//插入包装信息行数
	private int ubzxx_num = 0;//更新包装信息行数
	private int tbxx_num = 0;//插入同步信息行数
    private int ERROR_COUNT = 0;//错误信息行数
    
    private String sameJiaofdh = "";

    public EFIFhtzRead(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}
    
    
    /**
	 * 解析之前清空表数据
	 */
	@Override
	public void before() {
        try {
        	//EFI发货通知-发运主体信息表是否存在处理状态为1的数据
        	String jfdhTotal = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.selectObject("inPutzxc.queryJfdhTotal");
        	int i = Integer.parseInt(String.valueOf(strNull(jfdhTotal)));
			if(i>0){
				//清空EFI发货通知-同步零件信息表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.efiTbljxxDelete");
				//清空EFI发货通知-包装对应信息表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.efiBzdyxxDelete");
				//清空EFI发货通知-发运明细表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.efiFymxDelete");
	        	//清空EFI发货通知-发运主体信息表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutzxc.efiFyztxxDelete");
			}
		} catch (RuntimeException e) {
			logger.error("线程--接口" + dataParserConfig.getId() +"清除EFI发货通知相关表报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"清除EFI发货通知相关表报错"+e.getMessage());
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
		
		String FileBeginTime=DateTimeUtil.getAllCurrTime();///接口文件开始运行时间
    	String FileEndTime=null;//文件运行结束时间 
    	//add by pan.rui
    	List<String> dataList = new ArrayList<String>();
    	InputStreamReader is = null;
    	BufferedReader bufferedReader = null;
    	try {
    		 is = new InputStreamReader(fileInputStream,encodingStr);
    		//BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream,encodingStr));
    		 bufferedReader = new BufferedReader(is);
			String line = null;
			while((line = bufferedReader.readLine()) != null){
				//一个交付单号可对应多个零件，一个零件可以对应多个UA
	    		//参数解析
				try{
					this.ParseParameters(line);
				}catch(WarmBusinessException e){
					FileLog.getInstance(datasourceId).insert_file_ErrorInfo(getParamsErrorFileInfo(e.getMessage()),dataParserConfig.getBaseDao());//记录错误信息日志
					continue;
				}

	    		String jXhu = xuh + jfdh;
	    		String jxu = xuh + jfdh +ua;
	    		if(sameJiaofdh.equals(jfdh)){//文本中的交付单号和数据库中的交付单号相同
	    			continue;
	    		}
	    		if(!dataList.contains(jfdh)){
	    			//查询数据库中是否已存在相同数据，存在则记Log日志
	    			String jiaofdh = queryJiaofdh(jfdh);
	    			if(StringUtils.isEmpty(jiaofdh)){//数据库中不存在文本中的数据，写数据库
	    				//交付单号为空，则为第一条数据，全部插入
	    				insert(INSERT_FLAG_ALL);
	    			}else{//重复数据，写Log日志
	    				this.writeLog();
	    				sameJiaofdh = jiaofdh;
	    				continue;
	    			}
	    		}else if(dataList.contains(jfdh)&&!dataList.contains(jXhu)){
	    			//不插入IN_EFI_FHTZ_FYZTXX，插入明细和包装表
	    			insert(INSERT_FLAG_MXBZ);
	    		}else if(dataList.contains(jfdh)&&dataList.contains(jXhu)){
	    			//如果xuh_jfdh_ua相同的话，则更新
	    			if(dataList.contains(jxu)){
	    				insert(UPDATE_FLAG_BZ);
	    			}else{
	    				//插入IN_EFI_FHTZ_BZDYXX
	    				insert(INSERT_FLAG_BZ);
	    			}
	    		}else{
	    			//全部插入
	    			insert(INSERT_FLAG_ALL);
	    		}
	    		if(!dataList.contains(jfdh)){
	    			dataList.add(jfdh);
	    		}
	    		if(!dataList.contains(jXhu)){
	    			dataList.add(jXhu);
	    		}
	    		if(!dataList.contains(jxu)){
	    			dataList.add(jxu);
	    		}
	    		lineNum++;
			}
	    		FileEndTime=DateTimeUtil.getAllCurrTime();//文件运行结束时间  
	    		num=ztxx_num+fymx_num+bzxx_num+tbxx_num;//插入表数据总行数
				FileLog.getInstance(datasourceId).insert_file_info(getParamsFileInfo(fileName, FileBeginTime, FileEndTime,"1"),dataParserConfig.getBaseDao());//记录文件信息日志
    	}catch (Exception e) {
    		FileEndTime=DateTimeUtil.getAllCurrTime();//文件运行结束时间
			FileLog.getInstance(datasourceId).insert_file_info(getParamsFileInfo(fileName, FileBeginTime, FileEndTime,"-1"),dataParserConfig.getBaseDao());//记录文件信息日志
			FileLog.getInstance(datasourceId).insert_file_ErrorInfo(getParamsErrorFileInfo(e.getMessage()),dataParserConfig.getBaseDao());//记录错误信息日志
    		logger.error(e.getMessage());
    	} finally{
    		try{
    			after();
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
		params.put("update_num", String .valueOf(ubzxx_num));
		params.put("error_num", String .valueOf(ERROR_COUNT));
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
	 * 读取的行参数解析
	 * @author 贺志国
	 * @date 2012-10-19
	 * @param line 文本行数据
	 */
	public void ParseParameters(String line ){
		jfdh = line.substring(0, 17).trim();// 交付单号
		jf_date = line.substring(17, 31).trim();// 交付时间
		fyzmzdw = line.substring(31, 34).trim();// 发运总毛重单位
		fyyjdd_date = line.substring(34, 48).trim();// 发运预计到达日期时间
		cysdm = line.substring(48, 58).trim();// 承运商代码
		jszdm = line.substring(58, 78).trim();// 接收者代码
		xzddm = line.substring(78, 95).trim();// 卸载点代码
		fhrdm = line.substring(95, 115).trim();// 发货人代码
		jzxh = line.substring(115, 132).trim();// 集装箱号
		String tmp_sftbfhtz = line.substring(132, 134).trim();// 是否同步发货通知(1为不同步)
		sftbfhtz = "1".equals(tmp_sftbfhtz)?"0":"1"; // 是否同步发货通知，插入数据库要为0，所以此处作转换
		sfgzxlh = line.substring(134, 136).trim();// 是否关注序列号(1为关注)
		xuh = line.substring(136, 142).trim();// 序号
		String str_fysl =  line.substring(142, 157).trim(); // 发运的数量
		try{
			d_fysl = StringUtils.isEmpty(str_fysl)?0:Double.parseDouble(str_fysl);
		}catch(NumberFormatException e){
			logger.error("线程--接口" + interfaceId + "字符串转换异常  " + e.getMessage());
			throw new WarmBusinessException("字符串转转换错误！"+e.getMessage());
		}
		jldw = line.substring(157, 160).trim();// 发运数量计量单位
		bzlx = line.substring(160, 177).trim();// 包装类型
		String ljh1 = line.substring(177, 212).trim();// 零件号
		ljh = StringUtils.isEmpty(ljh1)?"":ljh1.substring(0, 10);
		ddh = line.substring(212, 229).trim();// 订单号
		lygj = line.substring(229, 232).trim();// 零件来源国家(位置来源)
		ua = line.substring(232, 249).trim();// UA
		ualx = line.substring(249, 254).trim();// UA类型
		uc = line.substring(254, 271).trim();// UC
		uclx = line.substring(271, 276).trim();// UC类型
		String uc_ljsl = line.substring(276, 286).trim(); // UC零件数量
		/**** 增加ua_ljsl 并优化代码   hzg 2012-10-19 ****/
		String ua_ljsl = line.substring(286, 296).trim(); // UA零件数量
		try{
			d_ucljsl = StringUtils.isEmpty(uc_ljsl)?0:Double.parseDouble(uc_ljsl); //UC零件数量
			d_ualjsl= StringUtils.isEmpty(ua_ljsl)?0:Double.parseDouble(ua_ljsl);// UA零件数量 ucljsl->ualjsl
		}catch(NumberFormatException e){
			logger.error("线程--接口" + interfaceId + "字符串转换异常  " + e.getMessage());
			throw new WarmBusinessException("字符串转转换错误！"+e.getMessage());
		}
		//d_ucljsl=ucljsl/1;
		//d_ualjsl=ualjsl/1;
		yhl = line.substring(296, 309).trim();// 要货令
		pzh = line.substring(309, 326).trim();// 批组号
		xlh = line.substring(326, 339).trim();// 序列号
		ggph=line.substring(339, 359).trim();//规格牌号
		//新增长宽高三个值
		chang = line.substring(359, 369).trim();//长
		kuan = line.substring(369,379).trim();//宽
		gao = line.substring(379,389).trim();//高
		gys = line.substring(389, 399).trim();// 供应商
		lsh = line.substring(399, 408).trim();// 流水号
		String t_shul = strNull(line.substring(408, 414).trim());
		shul = StringUtils.isEmpty(t_shul)?0:Integer.parseInt(t_shul);// 数量	
		
		//xss-0011589
		quancbqh = line.substring(414, 424).trim();// 全程标签US号 10
		xiaohd = line.substring(424, 437).trim(); // 全程消耗点 13
		xiaohcbh = line.substring(437, 442).trim(); // 小火车编号 5
		
		String t_tangc = strNull(line.substring(442, 444).trim());//趟次 2
		tangc = StringUtils.isEmpty(t_tangc)?0:Integer.parseInt(t_tangc); // 趟次 2
		
		shifht = line.substring(444, 445).trim();  // 是否混托（0：否，1：是） 1
		xiangd = line.substring(445, 446).trim();  // 巷道 1
		gongzr = line.substring(446, 460).trim();  // 工作日 14	 
		xianbck = line.substring(460, 466).trim();    //线边仓库 6 - 2015-9-17	 
		xianbckkw = line.substring(466, 472).trim();  //线边仓库库位6 - 2015-9-17
		changbiao = line.substring(472, 476).trim();    //厂标4 - 2015-9-17 
		cangkkw = line.substring(476, 482).trim(); 		 //定置库位6 2015-9-17 
		
		
		
	}
	
	/**
	 * 查询交付单号
	 * @author 贺志国
	 * @date 2013-1-9
	 * @param jiaofdh 交付单号
	 * @return String 查询到的交付单号
	 * @throws SQLException
	 */
	private String  queryJiaofdh(String jiaofdh){
		String strJfd = "";
		try{
			strJfd = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.selectObject("inPutzxc.queryJiaofdh",jiaofdh);
		} catch (RuntimeException e) {
			logger.error("线程--接口" + dataParserConfig.getId() +"交付单号查询表报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"交付单号查询报错"+e.getMessage());
		}
		return strNull(strJfd);
	}
	/**
	 * 写LOG日志
	 * @author 贺志国
	 * @date 2013-1-10
	 */
	private void writeLog(){
		StringBuilder buffer = new StringBuilder();
		buffer.append("交付单号已存在，交付单号jfdh="+jfdh);
		buffer.append(",jf_date="+strNull(jfdh)+",jf_date="+strNull(DateTimeUtil.SubString(jf_date)));
		buffer.append(",fyzmzdw="+strNull(fyzmzdw)+",fyyjdd_date="+strNull(DateTimeUtil.SubString(fyyjdd_date)));
		buffer.append(",cysdm="+strNull(cysdm)+",jszdm="+strNull(jszdm)+",xzddm="+strNull(xzddm));
		buffer.append(",fhrdm="+strNull(fhrdm)+",jzxh="+strNull(jzxh)+",sftbfhtz="+strNull(sftbfhtz));
		buffer.append(",sfgzxlh="+strNull(sfgzxlh)+",clzt=0");
		logger.info(buffer.toString());
	}
	/**
	 * 数据插入
	 * @author 贺志国
	 * @date 2012-10-19
	 * @param flag 1:插入发运主体信息表，2：插入发运明细表，3：插入包装对应信息表，4：更新包装对应信息表
	 * @throws SQLException
	 */
	public void insert(int flag) throws SQLException{ 
		try {
			Map<String,String> params = new HashMap<String,String>();
			params.put("xuh", strNull(xuh));
			params.put("jfdh", strNull(jfdh));
			params.put("jf_date",strNull(DateTimeUtil.SubString(jf_date)));
			params.put("fyzmzdw",strNull(fyzmzdw));
			params.put("fyyjdd_date",strNull(DateTimeUtil.SubString(fyyjdd_date)));
			params.put("cysdm", strNull(cysdm));
			params.put("jszdm", strNull(jszdm));
			params.put("xzddm", strNull(xzddm));
			params.put("fhrdm", strNull(fhrdm));
			params.put("jzxh", strNull(jzxh));
			params.put("sftbfhtz", strNull(sftbfhtz));
			params.put("sfgzxlh", strNull(sfgzxlh));
			params.put("currTime", DateTimeUtil.getAllCurrTime());			
		
			
			if(flag==1){
				params.put("changbiao", strNull(changbiao));//2015-9-1
				
				// 插入数据到EFI发货通知-发运主体信息表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.insertFyztxx",params);
				ztxx_num++;//记录插入行数
			}
			// 插入数据到EFI发货通知-发运明细表
			if(flag==1||flag==2){
				params.put("d_fysl", String.valueOf(d_fysl));
				params.put("jldw", strNull(jldw));
				params.put("bzlx", strNull(bzlx));
				params.put("ljh", strNull(ljh));
				params.put("ddh", strNull(ddh));
				params.put("lygj", strNull(lygj));
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.insertFymx",params);
				fymx_num++;//记录插入行数
			}
			
			// //插入数据到插入EFI发货通知-包装对应信息表
			if(flag==1||flag==2||flag==3){
				params.put("ua", strNull(ua));
				params.put("ualx", strNull(ualx));
				params.put("uc", strNull(uc));
				params.put("uclx", strNull(uclx));
				params.put("d_ucljsl", strNull(d_ucljsl));
				params.put("d_ualjsl", strNull(d_ualjsl));
				params.put("yhl", strNull(yhl));
				params.put("pzh", strNull(pzh));
				params.put("gys", strNull(gys));
				params.put("ggph", strNull(ggph));
				params.put("chang", strNull(chang));
				params.put("kuan", strNull(kuan));
				params.put("gao", strNull(gao));
				
				//xss-0011589
				params.put("quancbqh", strNull(quancbqh));
				params.put("xiaohd", strNull(xiaohd));
				params.put("xiaohcbh", strNull(xiaohcbh));
				params.put("tangc", strNull(tangc));
				params.put("shifht", strNull(shifht));
				params.put("xiangd", strNull(xiangd));
				params.put("gongzr", strNull(gongzr)); 
				params.put("xianbck", strNull(xianbck));//2015-9-17
				params.put("xianbckkw", strNull(xianbckkw)); //2015-9-17
				params.put("cangkkw", strNull(cangkkw)); //2015-9-17
				//params.put("beiz", strNull(beiz));
				
				
				
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.insertBzdyxx",params);
	            bzxx_num++;//记录插入行数
			}
			
			// //更新数据到插入EFI发货通知-包装对应信息表
			if(flag==4){
				//hanwu 20150725
				params.put("ua", strNull(ua));
				params.put("ualx", strNull(ualx));
				params.put("uc", strNull(uc));
				params.put("uclx", strNull(uclx));
				params.put("d_ucljsl", strNull(d_ucljsl));
				params.put("d_ualjsl", strNull(d_ualjsl));
				params.put("yhl", strNull(yhl));
				params.put("pzh", strNull(pzh));
				params.put("gys", strNull(gys));
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updateBzdyxx",params);
				ubzxx_num++;//更新的行数啊
			}
			// 是否同步发货通知为1时，则不进同步零件信息表
			// 插入数据到EFI发货通知-同步零件信息表
            if(INSERT_FLAG_TBLJ.equals(sftbfhtz)){
            	params.put("xlh", strNull(xlh));
            	params.put("lsh", strNull(lsh));
            	params.put("shul", strNull(shul));
            	params.put("lygj", strNull(lygj));
            	dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.insertTbljxx",params);
				tbxx_num++;//记录插入行数
            }
		} catch (RuntimeException e) {
			ERROR_COUNT++;//记录异常数据行数
			logger.error("线程--接口" + dataParserConfig.getId() +"更新出错，交付单号"+jfdh+"，UA号"+ua+"，顺序号"+xuh+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"更新出错，交付单号"+jfdh+"，UA号"+ua+"，顺序号"+xuh+e.getMessage());
		}
	}

	/**
	 * 空串处理
	 * @param obj对象
	 * @return 处理后字符串
	 * @author WL
	 * @date 2011-10-26
	 */
	private String strNull(Object obj) {// 对象为空返回空串,不为空toString
		return obj == null ? "" : obj.toString();
	}
	
	/**
	 * 查询数据插入到货通知单
	 * @param daohtzdList
	 * @throws SQLException
	 */
	public  void insertDaohtzd(List<Map<String,Object>> daohtzdList)throws SQLException{
		try {
			for(Map<String,Object> map:daohtzdList){
				String daoh_seqno = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.selectObject("inPutzxc.queryDaoh_seqno");
				String usercenter = (String) map.get("USERCENTER");
				String tch = (String) map.get("TCH");
				String chengysdm = (String) map.get("CHENGYSDM");
				String chengysmc = (String) map.get("CHENGYSMC");
				String yujddsj = (String) map.get("YUJDDSJ");
				String fayzmzdw = (String) map.get("FAYZMZDW");
				String psasj = (String) map.get("PASSJ");
				String changh = (String) map.get("CHANGH");
				//uth为用户中心后一位+0+序列
				String uth = usercenter.substring(1).trim()+daoh_seqno;
				map.put("UTH", uth);
			    //原始uth为uth
				map.put("YUANSUTH",uth);
				//这些字段进行为空替换
				map.put("TCH", strNull(tch));
				map.put("CREATOR", interfaceId);
				map.put("EDITOR", interfaceId);
				map.put("CHENGYSDM", strNull(chengysdm));
				map.put("CHENGYSMC", strNull(chengysmc));
				map.put("YUJDDSJ", strNull(yujddsj));
				map.put("FAYZMZDW", strNull(fayzmzdw));
				map.put("PASSJ", strNull(psasj));
				map.put("CHANGH", strNull(changh));
				//插入ck_daohtzd_dfpv表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.insertDaohtzd_dfpv", map);
				//更新Daohtzd_dfpv表的仓库编号和发运总数量
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updateDaohtzd_dfpv");
			}
		} catch (RuntimeException e) {
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"插入出错"+e.getMessage());
		}
	}
	
	/**
	 * 查询数据插入到uabq表
	 * @param uabqList
	 * @throws SQLException
	 */
	public void insertUabq(List<Map<String,Object>> uabqList)throws SQLException{
		try {
			List<String> dataList = new ArrayList<String>();
			Map<String,String> params = new HashMap<String,String>();
			for(Map<String,Object> map:uabqList){
				String usercenter = (String) map.get("USERCENTER");
				String gongysdm = strNull(map.get("GONGYSDM"));
				String blh = (String) map.get("BLH");
				String lingjbh = strNull((String) map.get("LINGJBH"));
				String pich = strNull((String) map.get("PICH"));
				//用来存放供应商+交付单号 和 零件+供应商+批次
				String ulh = "";
				String elh = "";
				//用来存放ulh和elh
				//查询是否存在相同的供应商和交付单号,供应商交付单号相同生成一个ulh
				if(!dataList.contains(gongysdm+blh)){
					String ul_seqno = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.selectObject("inPutzxc.queryul_seqno");
					ulh = usercenter.substring(1).trim()+ul_seqno;
					params.put(gongysdm+blh, ulh);
					dataList.add(gongysdm+blh);
				}else{
					ulh = params.get(gongysdm+blh);
				}
				//查询是否存在相同的供应商、零件号和批次号,供应商零件号和批次号相同生成一个el
				if(!dataList.contains(gongysdm+lingjbh+pich)){
					String el_seqno = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.selectObject("inPutzxc.queryel_seqno");
					elh = el_seqno;
					params.put(gongysdm+lingjbh+pich, elh);
					dataList.add(gongysdm+lingjbh+pich);
				}else{
					elh = params.get(gongysdm+lingjbh+pich);
				}
				int ucgs = 0;
				String lingjsl =  strNull(map.get("LINGJSL"));
				String uchl =  map.get("UCHL")==null?"0":map.get("UCHL").toString();	
				//如果uc容量为0或者空uc个数就为0;zx
				if(strNull(uchl).equals("")||Double.parseDouble(uchl)==0){
					ucgs = 0;
				}else{
					ucgs = (int)(Double.parseDouble(lingjsl)/Double.parseDouble(uchl));
				}
				String gongysmc = (String) map.get("GONGYSMC");
				String gongyslx = (String) map.get("GONGYSLX");
				String uaxh = (String) map.get("UAXH");
				String uarl = strNull(map.get("UARL"));
				String ucxh = (String) map.get("UCXH");
				String lingjmc = (String) map.get("LINGJMC");
				String cangkbh = (String) map.get("CANGKBH");
				String zickbh = (String) map.get("ZICKBH");
				String danw = (String) map.get("DANW");
				String zhuangtsx = (String) map.get("ZHUANGTSX");
				String chanx = (String) map.get("CHANX");
				String jingz = strNull(map.get("JINGZ"));
				String maoz =  strNull(map.get("MAOZ"));
				String yanssl = map.get("YANSSL").toString();
				String yaohlh = (String) map.get("YAOHLH");
				String dingdh = (String) map.get("DINGDH");
				String quancbqh = (String) map.get("QUANCBQH");
				String xiaohd = (String) map.get("XIAOHD");
				String dinghckkw = (String) map.get("DINGHCKKW");
				String xianbck = (String) map.get("XIANBCK");
				String xianbckkw = (String) map.get("XIANBCKKW");
				String daybs = (String) map.get("DAYBS");
				String shiftjkw = (String) map.get("SHIFTJKW");
				String xiaohcbh = (String) map.get("XIAOHCBH");
				String tangc =  strNull(map.get("TANGC"));
				String shifht = (String) map.get("SHIFHT");
				String xiangd = (String) map.get("XIANGD");
				String gongzr = (String) map.get("GONGZR");			
				map.put("GONGYSDM", gongysdm);
				map.put("LINGJBH", lingjbh);
				map.put("PICH", pich);
				map.put("UCHL", strNull(uchl));
				map.put("UCGS", strNull(ucgs));
				map.put("LINGJSL", strNull(lingjsl));
				map.put("UCGS", ucgs);
				map.put("ULH", ulh);
				map.put("ELH", elh);
				map.put("GONGYSMC", strNull(gongysmc));
				map.put("GONGYSLX", strNull(gongyslx));
				map.put("UAXH", strNull(uaxh));
				map.put("UARL", strNull(uarl));
				map.put("UCXH", strNull(ucxh));
				map.put("LINGJMC", strNull(lingjmc));
				map.put("CANGKBH", strNull(cangkbh));
				map.put("ZICKBH", strNull(zickbh));
				map.put("DANW", strNull(danw));
				map.put("ZHUANGTSX", strNull(zhuangtsx));
				map.put("CHANX", strNull(chanx));
				map.put("JINGZ", strNull(jingz));
				map.put("MAOZ", strNull(maoz));
				map.put("YANSSL", strNull(yanssl));
				map.put("YAOHLH", strNull(yaohlh));
				map.put("DINGDH", strNull(dingdh));
				map.put("QUANCBQH", strNull(quancbqh));
				map.put("XIAOHD", strNull(xiaohd));
				map.put("DINGHCKKW", strNull(dinghckkw));
				map.put("XIANBCK", strNull(xianbck));
				map.put("XIANBCKKW", strNull(xianbckkw));
				map.put("DAYBS", strNull(daybs));
				map.put("SHIFTJKW", strNull(shiftjkw));
				map.put("XIAOHCBH", strNull(xiaohcbh));
				map.put("TANGC", strNull(tangc));
				map.put("SHIFHT", strNull(shifht));
				map.put("XIANGD", strNull(xiangd));
				map.put("GONGZR", strNull(gongzr));
				map.put("CAOZY", interfaceId);
				map.put("CREATOR", interfaceId);
				map.put("EDITOR", interfaceId);
				//插入ck_uabq_dfpv表
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.insertUabq_dfpv", map);
				//更新ck_yaohl的要货令状态为02
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updateck_yaohl", map);
			}
		}
		catch (RuntimeException e) {
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"插入出错"+e.getMessage());
		}
		
	}
	
	
	/**
	 * 接口处理完之后处理方法
	 * @author yz
	 * @date 2015-12-23
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void after() {
		try {
			//查询需要插入ck_daohtzd_dfpv表的数据
			List<Map<String,Object>> daohtzdList = dataParserConfig.getBaseDao()
			.getSdcDataSource(datasourceId).select("inPutzxc.queryDaohtzd_dfpv");
			//判断是否为空，为空说明没有数据需要插入Daohtzd_dfpv表
			if(!daohtzdList.isEmpty()){
				insertDaohtzd(daohtzdList);
				//查询需要插入ck_daohtzd_dfpv表的数据
				List<Map<String,Object>> uabqList = dataParserConfig.getBaseDao()
				.getSdcDataSource(datasourceId).select("inPutzxc.queryUabq_dfpv");
				insertUabq(uabqList);
				for(Map<String,Object> map:daohtzdList){
					//更新in_efi_fhtz_fyztxx 的clzt为2
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.execute("inPutzxc.updatein_efi_fhtz_fyztxx", map);
				}
			}
		}catch (RuntimeException e) {
			logger.error("线程--接口" + interfaceId + "替换错误！  " + e.getMessage());
		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}	
}
