package com.athena.component.input;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
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
 * @modify 新增ck_daohtzd_dfpv，ck_uabq_dfpv 杨志 2015-1-13
 */
public class NeibuEFIFhtzRead extends TxtInputDBSerivce {
	protected static Logger logger = Logger.getLogger(NeibuEFIFhtzRead.class);	//定义日志方法
	
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
	
	private int num = 0;//插入总行数
	private int ztxx_num = 0;//插入主体信息行数
	private int fymx_num = 0;//插入发运明细行数
	private int bzxx_num = 0;//插入包装信息行数
	private int ubzxx_num = 0;//更新包装信息行数
	private int tbxx_num = 0;//插入同步信息行数
    private int ERROR_COUNT = 0;//错误信息行数
    
    private String sameJiaofdh = "";

    public NeibuEFIFhtzRead(DataParserConfig dataParserConfig) {
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
				after();
	    		FileEndTime=DateTimeUtil.getAllCurrTime();//文件运行结束时间  
	    		num=ztxx_num+fymx_num+bzxx_num+tbxx_num;//插入表数据总行数
				FileLog.getInstance(datasourceId).insert_file_info(getParamsFileInfo(fileName, FileBeginTime, FileEndTime,"1"),dataParserConfig.getBaseDao());//记录文件信息日志
    	}catch (Exception e) {    		
    		FileEndTime=DateTimeUtil.getAllCurrTime();//文件运行结束时间
			FileLog.getInstance(datasourceId).insert_file_info(getParamsFileInfo(fileName, FileBeginTime, FileEndTime,"-1"),dataParserConfig.getBaseDao());//记录文件信息日志
			FileLog.getInstance(datasourceId).insert_file_ErrorInfo(getParamsErrorFileInfo(e.getMessage()),dataParserConfig.getBaseDao());//记录错误信息日志
    		logger.error(e.getMessage());
    		throw new ServiceException("线程--接口" + interfaceId +"文本数据出错"+e.getMessage());
    	} finally{    		
    			try {
    				if(null!= bufferedReader){
    					bufferedReader.close();
    				}
					if (null != is) {
						is.close();
					}
					if (null != fileInputStream) {
						fileInputStream.close();
					}
				} catch (IOException e) {
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
		buffer.append(",sfgzxlh="+strNull(sfgzxlh)+",clzt=F");
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
	public  void insertDaohtzd(List<Map<String,Object>> daohtzdList){
		try {
			for(Map<String,Object> map:daohtzdList){
				//查询ck_daohtzd_dfpv表是否已经存在此数据
				int i = (Integer) dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.selectObject("inPutzxc.queryck_daohtzd_dfpv",map);
				//表示表中不存在此数据
				if(i<1){
					String daoh_seqno = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.selectObject("inPutzxc.queryDaoh_seqno");
					String usercenter = (String) map.get("USERCENTER");
					//uth为用户中心后一位+0+序列
					String uth = usercenter.trim().substring(1)+daoh_seqno;
					map.put("UTH", uth);
				    //原始uth为uth
					map.put("YUANSUTH",uth);
					String tch = (String) map.get("TCH");
					String chengysdm = (String) map.get("CHENGYSDM");
					String chengysmc = (String) map.get("CHENGYSMC");
					String yujddsj = (String) map.get("YUJDDSJ");
					String fayzmzdw = (String) map.get("FAYZMZDW");
					String psasj = (String) map.get("PASSJ");
					String changh = (String) map.get("CHANGH");
					String cangkbh = (String) map.get("CANGKBH");
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
					map.put("CANGKBH", strNull(cangkbh));
					//插入ck_daohtzd_dfpv表
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.execute("inPutzxc.insertDaohtzd_dfpv", map);
	        	}
			}
		} catch (RuntimeException e) {
			throw new ServiceException("线程--接口" + interfaceId +"插入ck_daohtz_dfpv出错"+e.getMessage());
		}
	}
	
	/**
	 * 查询数据插入到uabq表
	 * @param uabqList
	 * @throws SQLException
	 */
	public void insertUabq(List<Map<String,Object>> uabqList){
		try {
			List<String> dataList = new ArrayList<String>();
			Map<String,String> params = new HashMap<String,String>();
			for(Map<String,Object> map:uabqList){
				//查询uabq表是否已经存在此数据
				int i = (Integer) dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.selectObject("inPutzxc.queryck_uabq_dfpv",map);
				//没有此数据才插入uabq表
				if(i<1){
					String usercenter = (String) map.get("USERCENTER");
					String gongysdm = strNull(map.get("GONGYSDM"));
					String blh = (String) map.get("BLH");
					String lingjbh = strNull((String) map.get("LINGJBH"));
					String pich = strNull((String) map.get("PICH"));
					String gongysmc = strNull(map.get("GONGYSMC"));
					String gongyslx = strNull(map.get("GONGYSLX"));
					String xzddm = strNull(map.get("XZDDM"));
					String cysdm = strNull(map.get("CYSDM"));
					//新模式出库还有移库xzddm为仓库3位，供应商则取cysdm,供应商名称和供应商类型就取空
					if(xzddm.length()==3){
						gongysdm = cysdm;
						gongysmc = "";
						gongyslx = "";
					}
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
					//查询是否存在相同的供应商、零件号和批次号,供应商零件号和批次号相同生成一个elh
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
					String uaxh = (String) map.get("UAXH");
					String uarl = strNull(map.get("UARL"));
					String ucxh = (String) map.get("UCXH");
					String lingjmc = (String) map.get("LINGJMC");
					String cangkbh = (String) map.get("CANGKBH");
					String zickbh = (String) map.get("ZICKBH");
					String danw = (String) map.get("DANW");
					String zhuangtsx = (String) map.get("ZHUANGTSX");
					String chanx = (String) map.get("CHANX");
					if("YK".equals(map.get("JZXH"))){
						zhuangtsx = "YK";						
					}
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
					String changd = (String) map.get("CHANGD");
					if(StringUtils.isEmpty(changd)){
						changd = "1";
					}
					String kuand = (String) map.get("KUAND");
					if(StringUtils.isEmpty(kuand)){
						kuand = "1";
					}
					String gaod = (String) map.get("GAOD");
					if(StringUtils.isEmpty(gaod)){
						gaod = "1";
					}
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
					map.put("CHANGD", changd);
					map.put("KUAND", kuand);
					map.put("GAOD", gaod);
					//插入ck_uabq_dfpv表
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.execute("inPutzxc.insertUabq_dfpv", map);
	        	}
			}
		}
		catch (RuntimeException e) {
			throw new ServiceException("线程--接口" + interfaceId +"插入ck_uabq_dfpv出错"+e.getMessage());
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
			//更新in_efi_fhtz_fyztxx clzt=F为J
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutzxc.updatein_efi_fhtz_fyztxx_F");
			//查询需要插入ck_daohtzd_dfpv表的数据
			List<Map<String,Object>> daohtzdList = dataParserConfig.getBaseDao()
			.getSdcDataSource(datasourceId).select("inPutzxc.queryDaohtzd_dfpv");
			//查询需要插入ck_daohtzd_dfpv表的移库还有出库数据
			List<Map<String,Object>> daohtzd_jList = dataParserConfig.getBaseDao()
			.getSdcDataSource(datasourceId).select("inPutzxc.queryDaohtzd_dfpv_J");
			//判断是否为空，为空说明没有数据需要插入Daohtzd_dfpv表
			if(!daohtzdList.isEmpty()||!daohtzd_jList.isEmpty()){
				insertDaohtzd(daohtzdList);
				insertDaohtzd(daohtzd_jList);
				//查询需要插入ck_uabq_dfpv表的数据
				List<Map<String,Object>> uabqList = dataParserConfig.getBaseDao()
				.getSdcDataSource(datasourceId).select("inPutzxc.queryUabq_dfpv");
				insertUabq(uabqList);
				//更新in_efi_fhtz_fyztxx 传到DFPV的clzt为1
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updatein_efi_fhtz_fyztxx_dfpv");
				//更新in_efi_fhtz_fyztxx 其它的clzt为0
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updatein_efi_fhtz_fyztxx");
			}else{
				//更新in_efi_fhtz_fyztxx 其它的clzt=J为0
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutzxc.updatein_efi_fhtz_fyztxx");
			}
		}catch (RuntimeException e) {
			logger.error(e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId +"出错"+e.getMessage());
		} 
	}
}
