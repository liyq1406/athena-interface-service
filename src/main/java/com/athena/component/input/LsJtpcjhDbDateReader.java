package com.athena.component.input;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DbDataWriter;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.util.exception.ServiceException;
import com.athena.util.uid.CreateUid;


/**
 * 3060 九天排产计划(JT的顺序)
 * @author hzg
 *
 */
public class LsJtpcjhDbDateReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(LsJtpcjhDbDateReader.class);	//定义日志方法
	protected List<Map<String,String>> shangXJSList = new ArrayList<Map<String,String>>();
	protected String kanBanSJ = null; 
	protected int buZ = 0; //步长
	private String logInfo = "";
	private String logDate = "";
	private String datasourceId="";
	public LsJtpcjhDbDateReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}

	private List<String> userCenters = null;
	/**
	 * 解析数据之前清空in_jtpcjh表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.jtpcjhDelete");
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + dataParserConfig.getId() +"清除in_jtpcjh表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"清除in_jtpcjh表时报错"+e.getMessage());
		}
	}

	/**
	 * 行解析前处理
	 */
	@Override
	public boolean beforeRecord(String line, String fileName, int lineNum){
		boolean result = true;
		if (lineNum==1||StringUtils.isEmpty(line)) {// 文件第一行不导入表
			result =  false;
		}else{
			try{
				String user = line.toString().substring(0, 2).trim();
				if(userCenters!=null&&userCenters.size()>0){
					if(userCenters.contains(user)){
						result = true;
					}else{
						result =  false;
					}
				}
			}catch(Exception e){
				logger.error(line+"线程--接口" + dataParserConfig.getId()+e.getMessage(),e);
			}
		}
		return result;
	}


	//提取上线时间数据 add by pan.rui
	@SuppressWarnings({"unchecked" })
	private List<Map<String,String>> GetShangXSJ(String usercenter,String sxc,String yjjhzrq){
		List<Map<String,String>> shangXSJ=null;
		try{
			Map<String,String> params = new HashMap<String,String>();
			params.put("usercenter", strNull(usercenter));
			params.put("chanx", strNull(sxc));
			params.put("yjjhzrq", strNull(yjjhzrq));
			shangXSJ = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.queryJuedskOfGongzsjmb",params);
		}catch(Exception e){
			logger.error("线程--接口" + dataParserConfig.getId() +"提取工作时间模板上线时间数据时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"提取工作时间模板上线时间数据报错"+e.getMessage());
		}
		return shangXSJ;
	}
	
	/**
	 * 根据用户中心和生产线取整车生产节拍
	 * @author Hezg
	 * @date 2013-3-11
	 * @param usercenter 用户中心
	 * @param sxc 生产线
	 * @return int 生产节拍
	 */
	private int GetJieP(String usercenter,String sxc){
		int buz = 0;
		int jiep = 0;
		double d_buz = 0;
		String shengcjp = "";
		Map<String,String> params = new HashMap<String,String>();
		params.put("usercenter", strNull(usercenter));
		params.put("shengcxbh", strNull(sxc));
		try{	
			 shengcjp = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutddbh.queryJiepOfShengcx",params);
		}catch(Exception e){
			logInfo = "查询生产节拍异常";
			logger.error("线程--接口" + dataParserConfig.getId() +"根据用户中心和生产线取整车生产节拍报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"根据用户中心和生产线取整车生产节拍报错"+e.getMessage());
		}
		if(StringUtils.isNotEmpty(shengcjp)){
			jiep = Integer.parseInt(shengcjp);
			d_buz = (double)60 / jiep;
			BigDecimal mData = new BigDecimal(String.valueOf(d_buz)).setScale(0, BigDecimal.ROUND_HALF_UP);
			buz = Integer.parseInt(String.valueOf(mData));
		}
		return buz;
	}
	
	//是否存在coddc数据
	private boolean isExistsCoddc(){
		boolean flag = false;
		try {
			String clddxxCount = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).selectObject("inPutddbh.queryCoddcnumOfJtpc");
			int coddcNum = Integer.valueOf(clddxxCount);
			if(coddcNum > 0){
				flag = true; //存在无匹配CODDC
			}
		} catch (RuntimeException e) {
			logInfo = "存在无匹配CODDC，请查看是否表(IN_JTPCJH,IN_CLDV_CODDC)数据存在业务问题！";
			logger.error(e.getMessage());
		}
		return flag;
	}
	
	//插入表进行数据更新
	@SuppressWarnings("unchecked")
	@Override
	public void after() {
		String SID=DbDataWriter.getUUID();//唯一标示
		String EID=DbDataWriter.getUUID();//唯一标示
		List<Map<String,String>> inList = null;
		try{
			inList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.queryOfhOfJtpcLs");
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"查询IN_JTPCJH报错"+e.getMessage());
			File_ErrorInfo(EID,"3060",SID,"计算上线时间和开班时间出错！",logDate);
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询IN_JTPCJH报错"+e.getMessage());
		}
		if(!inList.isEmpty()){
			DealShangxsjKaibsj(inList);
			if(isExistsCoddc()){
				logInfo = "无匹配CODDC，请查看是否表(IN_JTPCJH,IN_CLDV_CODDC)数据存在业务问题！";
				File_ErrorInfo(EID,"3200",SID,logInfo,"");
			}
		}else{
			logInfo = "用户中心不这匹配，无数据！";
			File_ErrorInfo(EID,"3200",SID,logInfo,logDate);
			return;
		}
	}

	/**
	 * 查询IN_CLDDXX表并更新上线时间和开班时间
	 * @author Hezg
	 * @date 2013-3-12
	 * @param inList
	 */
	private void DealShangxsjKaibsj(List<Map<String,String>> inList){
		String d_shangXSJ = "";
		String compDate = "";
		int num = 1;
		int sxxuh = 0;
		for(int i=0;i<inList.size();i++){
			String usercenter = strNull((inList.get(i)).get("USERCENTER")); //用户中心
			String zzx = strNull((inList.get(i)).get("ZZX")); //总装生产线号
			String scxh = usercenter +"5"+zzx;
			String YJJHZRQ = strNull((inList.get(i)).get("JTRQ")).substring(0,10);//JT时间
			String hzxuh = strNull((inList.get(i)).get("JTXX")); //JT顺序号
			String tmp_compDate = usercenter+scxh+YJJHZRQ;
			if(StringUtils.isNotEmpty(hzxuh)){
				sxxuh = Integer.parseInt(hzxuh);
			}
			String whof = strNull((inList.get(i)).get("OFH")); //of单号
			logDate = "用户中心:"+usercenter+";OF号:"+whof+";预计焊装时间："+YJJHZRQ;
			if(sxxuh == 0){
				continue;
			}
			if(compDate.equals(tmp_compDate)){
				num++;
			}else{
				num = 1;
			}
			if(num==1){
				shangXJSList = GetShangXSJ(usercenter,scxh,YJJHZRQ);
				buZ = GetJieP(usercenter,scxh);
				if(shangXJSList.size()>0){
					d_shangXSJ = strNull((shangXJSList.get(0)).get("JUEDSK"));
					kanBanSJ = d_shangXSJ;
					updateShangxsjKaibsj(d_shangXSJ,num,usercenter,whof);
				}else{
					logInfo = "无计算上线时间和开班时间！";
					File_ErrorInfo(DbDataWriter.getUUID(),"3200",DbDataWriter.getUUID(),logInfo,logDate);
					insertDDBH_YICBJ("3200",usercenter,"九天排产计划(JT的顺序)","2",logInfo); //记录用户异常报警
					continue;
				}
			} else{
				if(shangXJSList.size()>0){
					int xuh = (num -1) * buZ;
					d_shangXSJ = strNull((shangXJSList.get(xuh)).get("JUEDSK"));
					updateShangxsjKaibsj(d_shangXSJ,num,usercenter,whof);
				}
			}
			compDate = tmp_compDate;
		}
	}

	/**
	 * 更新上线时间和开班时间
	 * @author Hezg
	 * @date 2013-3-12
	 * @param d_shangXSJ 上线时间
	 * @param num  上线序号
	 * @param usercenter 用户中心
	 * @param whof 订单号
	 */
	private void updateShangxsjKaibsj(String d_shangXSJ,int num,String usercenter,String whof){
		Map<String,String> params  = new HashMap<String,String>();
		params.put("d_shangXSJ",d_shangXSJ);
		params.put("kanBanSJ",kanBanSJ);
		params.put("num",String.valueOf(num));
		params.put("usercenter",usercenter);
		params.put("whof",whof);
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.updateIN_JTPCJH",params);
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"更新IN_JTPCJH表上线时间和开班时间报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"更新IN_JTPCJH表上线时间和开班时间报错"+e.getMessage());
		}
	}

	
	
	/**
	 * 记录数据日志表
	 */
	public void File_ErrorInfo(String EID,String CID,String SID,String file_errorinfo,String error_date){
		Map<String,String> params = new HashMap<String,String>();
		params.put("EID",  strNull(EID));
		params.put("INBH", strNull(CID));
		params.put("SID", strNull(SID));
		params.put("file_errorinfo", strNull(file_errorinfo));
		params.put("error_date", strNull(error_date));
		try {
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.insertErrorFileInfo",params);
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"写in_errorfile表报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"写in_errorfile表报错"+e.getMessage());
		}
	}
	
	/**
	 * 记录用户异常报警日志表
	 * DbUtils.execute(strbuf.toString(),datasourceid);
	 * Mantis 0006532 修改
	 * jsmk 计算模块,cId 接口编号
	 * @return
	 */
	 public void insertDDBH_YICBJ(String interID,String usercenter,String chnname,String yclx,String ycxx){
		Map<String,String> params = new HashMap<String,String>();
		params.put("cId", CreateUid.getUID(20));
		params.put("usercenter", strNull(usercenter));
		params.put("exenanme",  interID);
		params.put("chnname",  chnname);
		params.put("yclx", strNull(yclx));
		params.put("ycxx", strNull(ycxx));
		try {
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.insertDDBH_YICBJ",params);
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"写DDBH_YICBJ表报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"写DDBH_YICBJ表报错"+e.getMessage());
		}
	}
	
	
	
	/**
	 * 得到配置文件的用户中心
	 * @author Hezg
	 * @date 2013-3-7
	 * @return List<String> 用户中心List集合
	 */
	@SuppressWarnings("unused")
	private List<String> GetUserCenter(){
		InputStream  in = ClddxxDbDataWriter.class.getResourceAsStream("/config/exchange/urlPath.properties");
		Properties pp = new Properties();
		List<String> userCenters = new ArrayList<String>();
		try {
			pp.load(in);
			String usercenter = pp.getProperty("usercenter");
			if(usercenter.contains(",")){
				String[] users = usercenter.split(",");
				if(users!=null&&users.length>0){
					for(int i=0;i<users.length;i++){
						if(StringUtils.isEmpty(strNull(users[i]))){
							userCenters.add(users[i]);
						}
					}
				}
			}else{
				userCenters.add(usercenter);
			}
			
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return userCenters;
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
