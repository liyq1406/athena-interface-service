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

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.util.exception.ServiceException;

import edu.emory.mathcs.backport.java.util.Arrays;

/**
 * 3050 九天排产计划(商业化的时间)
 * @author hzg
 *
 */
public class ClddxxDbDataReader extends TxtInputDBSerivce{
	protected static Logger logger = Logger.getLogger(ClddxxDbDataReader.class);	//定义日志方法
	private String datasourceId = "";
	public ClddxxDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();

	}
	protected List<Map<String,String>> shangXJSList = new ArrayList<Map<String,String>>();
	private List<String> userCenters = null;
	protected Record jpRecord = new Record();
	protected String kanBanSJ = null; 
	private String logInfo = "";
	private String logDate = "";
	protected double buZ = 0; //步长    int改为double类型
	/**
	 * 解析数据之前清空in_clddxx和ddbh_shangyhsjjh表数据
	 */
	@Override
	public void before() {
		try{
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.clddxxDelete");
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.shangyhsjjhDelete");
			userCenters = getUserCenter();
		}catch(RuntimeException e)
		{
			logger.error("线程--接口" + dataParserConfig.getId() +"清除in_clddxx和ddbh_shangyhsjjh表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"清除in_clddxx和ddbh_shangyhsjjh表时报错"+e.getMessage());
		}
	}

	/**
	 * 行解析前处理
	 */
	@Override
	public boolean beforeRecord(String line, String fileName, int lineNum){
		boolean result = true;
		String user = line.toString().substring(0, 2).trim();
		if(userCenters!=null&&userCenters.size()>0){
			if(userCenters.contains(user)){
				result = true;
			}else{
				result = false;
			}
		}
		return result;
	}


	/**
	 * 得到配置文件的用户中心
	 * @author Hezg
	 * @date 2013-3-7
	 * @return List<String> 用户中心List集合
	 */
	@SuppressWarnings("unchecked")
	private List<String> getUserCenter(){
		InputStream  in = ClddxxDbDataReader.class.getResourceAsStream("/config/exchange/urlPath.properties");
		Properties pp = new Properties();
		List<String> userCenters = new ArrayList<String>();
		try {
			pp.load(in);
			String usercenter = pp.getProperty("usercenter");
			if(usercenter.contains(",")){
				String[] users = usercenter.split(",");
				userCenters = Arrays.asList(users);
			}else{
				userCenters.add(usercenter);
			}
			
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return userCenters;
	}
	
	/**
	 * 提取上线时间数据 add by pan.rui
	 * @author Hezg
	 * @date 2013-3-7
	 * @param usercenter 用户中心
	 * @param sxc 生产线
	 * @param yjjhzrq 预计计划周日期
	 * @return List<Map<String,String> 上线时间集合
	 */
	@SuppressWarnings({"unchecked" })
	private List<Map<String,String>> GetShangXSJ(String usercenter,String sxc,String yjjhzrq){
		List<Map<String,String>> shangXSJ=null;
		try{
			Map<String,String> params = new HashMap<String,String>();
			params.put("usercenter", strNull(usercenter));
			params.put("chanx", strNull(sxc));
			params.put("yjjhzrq", strNull(yjjhzrq));
			shangXSJ = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.select("inPutddbh.queryJuedskOfGongzsjmb",params);
		}catch(Exception e){
			logger.error("线程--接口" + dataParserConfig.getId() +"提取工作时间模板上线时间数据时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"提取工作时间模板上线时间数据报错"+e.getMessage());
		}
		return shangXSJ;
	}
	
	/**
	 * 根据用户中心和生产线取整车生产节拍计算步长
	 * @author Hezg
	 * @date 2013-3-7
	 * @param usercenter 用户中心
	 * @param sxc 生产线
	 * @return double  步长
	 */
	private double GetJieP(String usercenter,String sxc){
		double buz = 0;  //int 改为double hzg 2013-11-6
		int jiep = 0;
		double d_buz = 0;
		String shengcjp = "";
		Map<String,String> params = new HashMap<String,String>();
		params.put("usercenter", strNull(usercenter));
		params.put("shengcxbh", strNull(sxc));
		try{	
			 shengcjp = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.selectObject("inPutddbh.queryJiepOfShengcx",params);
		}catch(Exception e){
			logInfo = "查询生产节拍异常";
			logger.error("线程--接口" + dataParserConfig.getId() +"根据用户中心和生产线取整车生产节拍报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"根据用户中心和生产线取整车生产节拍报错"+e.getMessage());
		}
		if(StringUtils.isNotEmpty(shengcjp)){
			jiep = Integer.parseInt(shengcjp);
			d_buz = (double)60 / jiep;
//			BigDecimal mData = new BigDecimal(String.valueOf(d_buz)).setScale(0, BigDecimal.ROUND_HALF_UP);
//			buz = Integer.parseInt(String.valueOf(mData));
			BigDecimal mData = new BigDecimal(String.valueOf(d_buz)).setScale(2, BigDecimal.ROUND_HALF_UP);
			buz = mData.doubleValue();
		}

		return buz;
	}
	
	
	/**
	 * 生产线是否都具有生产节拍
	 * @author Hezg
	 * @date 2013-3-7
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public boolean getJPMap(){
		boolean flag = true;
		try{
			List<Map<String,String>> inList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.select("inPutddbh.queryHanzscxOfClddxx");
			String jpDate = "";
			if(inList != null && inList.size()>0){
				for(int i=0;i<inList.size();i++){
					String usercenter = strNull((inList.get(i)).get("USERCENTER")); //用户中心
					String scxh = strNull((inList.get(i)).get("HANZSCX")); //焊装生产线号
					jpDate = usercenter+scxh;
					logDate = "用户中心："+usercenter+";产线："+scxh; 
					jpRecord.put(jpDate, GetJieP(usercenter, scxh));
				}
			}else{
				logInfo = "无法匹配CKX_CHEXPT的悍装生产线，没有数据入库，请确认！";
				flag = false;
			}
		}catch(Exception e){
			flag = false;
			logInfo = "生产节拍计算异常！【inPutddbh.queryHanzscxOfClddxx】查询出错！";
			logger.error("线程--接口" + dataParserConfig.getId() +"判断生产线是否都具有生产节拍异常"+e.getMessage());
		}
		return flag;
	}
	
	/**
	 * 需要判断是否不存生产线的数据
	 * @author Hezg
	 * @date 2013-3-7
	 * @return
	 */
	private boolean IsGetShengCX(){
		boolean flag = true;
		try{
			String clddxxCount = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.selectObject("inPutddbh.queryCountOfClddxx");
			int cxNum = Integer.parseInt(clddxxCount);
			if(cxNum > 0){
				logInfo = "存在【"+cxNum+"】条数据无法匹配的生产线(HANZSCX)，请查看是否表(IN_CLDDXX,CKX_CHEXPT)数据存在业务问题！";
				flag = false;
			}
		}catch(Exception e){
			flag = false;
			logInfo = "无匹配的生产线(HANZSCX)，请查看是否表(IN_CLDDXX,CKX_CHEXPT)数据存在业务问题！";
			logger.error("线程--接口" + dataParserConfig.getId() +"无匹配的生产线(HANZSCX)，请查看是否表(IN_CLDDXX,CKX_CHEXPT)数据存在业务问题"+e.getMessage());
		}
		return flag;
	}
	
	
	/**
	 * 是否存在coddc数据
	 * @author Hezg
	 * @date 2013-3-7
	 * @return boolean
	 */
	private boolean isExistsCoddc(){
		boolean flag = false;
		String strNum = "";
		try {
			strNum = (String)dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.selectObject("inPutddbh.queryCoddcnum");
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"查询是否存在coddc数据报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询是否存在coddc数据报错"+e.getMessage());
		}
		int coddcNum = Integer.valueOf(strNull(strNum));
		if(coddcNum > 0){
			logInfo = "存在【"+coddcNum+"】条数据无法匹配coddc！";
			flag = true;
		}
		return flag;
	} 
	
	//插入表进行数据更新
	/*@SuppressWarnings("unchecked")
	@Override
	public void after() {
		String SID=TxtWriterDBTask.getUUID();//唯一标示
		String EID=TxtWriterDBTask.getUUID();//唯一标示
		List<Map<String,String>> inList = null;
		try{
			//首先判断是否存在没有生产线的数据 
			if(!IsGetShengCX()){
				File_ErrorInfo(EID,"3050",SID,logInfo,"");
			}
			//然后查询是否存在生产节拍 
			//如果有生产节拍数据，则进行上线时间和开班时间操作
			if(getJPMap()){
				try {
					inList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.select("inPutddbh.queryWhof");
				}catch(RuntimeException e){
					logger.error("线程--接口" + dataParserConfig.getId() +"查询IN_CLDDXX表WHOF报错"+e.getMessage());
					throw new ServiceException("线程--接口" + dataParserConfig.getId() +"查询IN_CLDDXX表WHOF报错"+e.getMessage());
				}
				if(inList != null){
					DealShangxsjKaibsj(inList);
				}else{
					String EID_3=TxtWriterDBTask.getUUID();//唯一标示
					logInfo = "无匹配的用户中心数据，请查看是否表(IN_CLDDXX)数据存在业务问题！";
					File_ErrorInfo(EID_3,"3050",SID,logInfo,logDate);
					return;
				}
				if(isExistsCoddc()){
					String EID_4=TxtWriterDBTask.getUUID();//唯一标示
					File_ErrorInfo(EID_4,"3050",SID,logInfo,"");
				}
			}else{
				String EID_5=TxtWriterDBTask.getUUID();//唯一标示
				File_ErrorInfo(EID_5,"3050",SID,logInfo,logDate);
			}
		}catch(Exception e){
			logger.error(e.getMessage());
			if(StringUtils.isEmpty(logInfo)){
				logInfo = "计算上线时间和开班时间出错！";
			}
			File_ErrorInfo(EID,"3050",SID,logInfo,logDate);
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +e.getMessage());
		}
	}*/
	
	/**
	 * 查询IN_CLDDXX表并更新上线时间和开班时间
	 * @author Hezg
	 * @date 2013-3-12
	 * @param inList
	 */
	private void DealShangxsjKaibsj(List<Map<String,String>> inList){
		String updateFlag = "";
		String d_shangXSJ = "";
		String compDate = "";
		int num = 1;
		int sxxuh = 0;
		int maxNum = 0;
		for(int i=0;i<inList.size();i++){
			String usercenter = strNull((inList.get(i)).get("USERCENTER")); //用户中心
			String scxh = strNull((inList.get(i)).get("HANZSCX")); //焊装生产线号
			String YJJHZRQ = strNull((inList.get(i)).get("YJJHZRQ")).substring(0,10);//进入焊装时间
			String hzxuh = strNull((inList.get(i)).get("SXSXH")); //循环号
			String tmp_compDate = usercenter+scxh+YJJHZRQ;  //给IN_CLDDXX表编号用的
			String tmp_JP = usercenter+scxh; //给JP的一个临时变量 ，通过该变量获得步长值 
			if(StringUtils.isNotEmpty(hzxuh)){
				sxxuh = Integer.parseInt(hzxuh);
			}
			String whof = strNull((inList.get(i)).get("WHOF")); //of单号
			logDate = "用户中心:"+usercenter+";OF号:"+whof+";预计焊装时间："+YJJHZRQ;
			if(sxxuh == 0){
				continue;
			}
			//给拆分业务数据做编号，相同用户中心、焊装产线，不同焊装日期的数据要要排序
			if(compDate.equals(tmp_compDate)){
				num++;
			}else{
				num = 1;
			}
			if(num==1){
				//buZ = Integer.parseInt(jpRecord.getString(tmp_JP));  --For input string: "3.75"
				buZ = Double.valueOf(jpRecord.getString(tmp_JP)); 
				shangXJSList = GetShangXSJ(usercenter,scxh,YJJHZRQ);
				maxNum = shangXJSList.size();
				String EID_1=TxtWriterDBTask.getUUID();//唯一标示
				if(buZ!=0){
					if(maxNum>0){
						d_shangXSJ = strNull((shangXJSList.get(0)).get("JUEDSK"));
						kanBanSJ = d_shangXSJ;
						updateShangxsjKaibsj(d_shangXSJ, num, usercenter, whof);
						updateFlag = "true";
					}else{
						logInfo = "无计算上线时间和开班时间！";
						File_ErrorInfo(EID_1,"3050",TxtWriterDBTask.getUUID(),logInfo,logDate);
					}
				}else{
					File_ErrorInfo(EID_1,"3050",TxtWriterDBTask.getUUID(),logInfo,logDate);
				}
			} else{
				if(buZ!=0){
					if(maxNum>0){
						//int xuh = (num -1) * buZ;
						double xuh = Math.floor((num -1) * buZ);  //向下取整  hzg 2013-11-6 mantis:8837
						if(xuh<= maxNum){
							d_shangXSJ = strNull((shangXJSList.get((int)xuh)).get("JUEDSK"));
							updateShangxsjKaibsj(d_shangXSJ, num, usercenter, whof);
							updateFlag = "true";
						}
					}
				}
			}	
			if(StringUtils.isEmpty(updateFlag)){
				logInfo = "(无计算上线时间和开班时间或步长过大)！";
				String EID_2=TxtWriterDBTask.getUUID();//唯一标示
				File_ErrorInfo(EID_2,"3050",TxtWriterDBTask.getUUID(),logInfo,logDate);
				return;
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
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.updateIN_CLDDXX",params);
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"更新IN_CLDDXX表上线时间和开班时间报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"更新IN_CLDDXX表上线时间和开班时间报错"+e.getMessage());
		}
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
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.insertErrorFileInfo",params);
		}catch(RuntimeException e){
			logger.error("线程--接口" + dataParserConfig.getId() +"写in_errorfile表报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"写in_errorfile表报错"+e.getMessage());
		}
	}
	
}
