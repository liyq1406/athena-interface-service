package com.athena.component.service.imp;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.jws.WebService;

import org.apache.log4j.Logger;

import com.athena.ckx.util.Impl.CkxCommonFuncImpl;
import com.athena.component.exchange.DataExchange;
import com.athena.component.exchange.FileNotFindException;
import com.athena.component.exchange.InziDbUtils;
import com.athena.component.output.DuoJieKouOutputWriter;
import com.athena.component.service.DispatchService;
import com.athena.component.service.SysmonitorService;
import com.athena.component.service.bean.InterfaceConfig;
import com.athena.component.service.bean.OrderConfig;
import com.athena.component.service.utls.InfaceParserConfig;
import com.athena.fj.module.service.FJScheduleService;
import com.athena.fj.module.service.YaocjhService;
import com.athena.pc.module.webInterface.PCDailyProduceService;
import com.athena.pc.module.webInterface.PCDailyRollService;
import com.athena.pc.module.webInterface.PCLeijjfService;
import com.athena.pc.module.webInterface.PCScheduleService;
import com.athena.xqjs.module.anxorder.service.AnxJisService;
import com.athena.xqjs.module.anxorder.service.XinaxJisService;
import com.athena.xqjs.module.denglswtx.service.DenglswtxService;
import com.athena.xqjs.module.fenzxpc.service.FenzxpcService;
import com.athena.xqjs.module.hlorder.service.MJCalculateService;
import com.athena.xqjs.module.hlorder.service.MaoxqcfService;
import com.athena.xqjs.module.hlorder.service.VJCalculateService;
import com.athena.xqjs.module.hlorder.service.VJMaoxqcfService;
import com.athena.xqjs.module.ilorder.service.PJCalculateService;
import com.athena.xqjs.module.kanbyhl.service.KanbjsService;
import com.athena.xqjs.module.maoxq.action.JlvXinaxmpareAction;
import com.athena.xqjs.module.maoxq.service.DdbhClvCompareService;
import com.athena.xqjs.module.maoxq.service.JlvXinaxCompareService;
import com.athena.xqjs.module.quhysfy.service.RukmxService;
import com.athena.xqjs.module.ziyhqrq.ZiyhqrqService;
import com.toft.core3.container.annotation.Component;
import com.toft.core3.container.annotation.Inject;
import com.toft.core3.ibatis.support.AbstractIBatisDao;

@Component
@WebService(endpointInterface="com.athena.component.service.DispatchService",serviceName="/dispatchServiceImpl")
public class DispatchServiceImpl implements DispatchService {
	protected static Logger logger = Logger.getLogger(DispatchServiceImpl.class);	//定义日志方法
	//private static String urlPath = null;
	@Inject
	private DataExchange dataEchange;
	@Inject 
	protected AbstractIBatisDao baseDao;//注入baseDao
	
	//引入批量对象  hzg 2014-1-14
	@Inject
	private CkxCommonFuncImpl ckxCFClient;
	@Inject
	private PJCalculateService xqjsPJClient;
	@Inject
	private KanbjsService xqjsKbClient;
	@Inject
	private ZiyhqrqService xqjsZykzClient;
	@Inject 
	private AnxJisService xqjsAxClient;
	@Inject 
	private KdysService kdysClient;
	@Inject 
	private PCLeijjfService pclClient;
	@Inject 
	private PCDailyRollService pcdClient;
	@Inject 
	private PCScheduleService pcscLient;
	@Inject 
	private PCDailyProduceService pcpClient;
	@Inject 
	private YaocjhService fjYcjhClient;
	@Inject 
	private FJScheduleService fjsClient;
	@Inject 
	private DenglswtxService xqjsDltxClient;
	@Inject 
	private DdbhClvCompareService xqjsDdbhclvClient;
	@Inject 
	private JlvXinaxCompareService xqjsJlvXinaxClient;
	@Inject 
	private FenzxpcService fenzxpcClient;
	
	@Inject 
	private XinaxJisService xqjsXaxClient;
	
	//hml 2015-12-12注入
	@Inject
	private VJMaoxqcfService vjmaoxqcfClient;
	
	//hml 2015-12-12注入
	@Inject
	private MaoxqcfService maoxqcfClient;
	
	//wyg VJ计算服务 2015-12-16 注入
	@Inject
	private VJCalculateService vJCalculateService ;
	
	//wyg VJ计算服务 2015-12-16 注入
	@Inject
	private MJCalculateService mJCalculateService ;
	
	//hzg 2013-7-24注入
	@Inject
	private DuoJieKouOutputWriter djkWriter;
	
	private final static int NOT_FOUND_FILE =  99;  //没有找到文件报错
	private final static int EXP_ERROR =  1;  //程序错误
	
	private final static String RUN_ERROR_STATE = "2"; //接口运行出错
	//private final static String RUN_SUSS_STATE = "0"; //接口运行出错
	
	
	
	@Inject 
	private RukmxService rukmxService;
	
	/**
	 * 一次执行一条接口命令
	 * 	返回值： 1 说明运行出现严重错误
	 *          99：说明找到文件
	 *          0：运行结束
	 */
	@Override
	public int dispatchTask(String taskName) {
		if(!taskName.startsWith("4")){
			return doInterface(taskName);
		}else{
			return doZBC(taskName);
		}
	}
	
	/**
	 * 调用准备层业务-webservice
	 * @param taskName
	 * @return
	 */
	private int doZBC(String taskName) {
		logger.info("开始调用批量"+taskName);
		int result = 1; //将 0 -->1 hzg 2014.5.28
		
		/*if(urlPath==null){
			InputStream  in = DispatchServiceImpl.class.getResourceAsStream("/config/exchange/urlPath.properties");
			Properties pp = new Properties();
			try {
				pp.load(in);
				urlPath = pp.getProperty("urlPath");
//				System.out.println("urlPath =  " + urlPath);
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}*/
		
		try{
			if("4020".equals(taskName)){
				//参考系：未来几日处理
				logger.info("批量-未来几日处理"+taskName+"调用服务端开始");
				ckxCFClient.calculate();
				logger.info("批量-未来几日处理"+taskName+"调用服务端结束");
			}else if("4030".equals(taskName)){
				//参考系：物流路径总图处理
				logger.info("批量-物流路径"+taskName+"调用服务端开始");
				ckxCFClient.addWullj();
				logger.info("批量-物流路径"+taskName+"调用服务端结束");
			}else if("4040".equals(taskName)){
				//参考系：小火车时刻表生成
				logger.info("批量-小火车时刻表生成"+taskName+"调用服务端开始");
				ckxCFClient.calculateXiaohcYssk();
				logger.info("批量-小火车时刻表生成"+taskName+"调用服务端结束");
			}else if("4050".equals(taskName)){
				//参考系：生效期处理
				logger.info("批量-生效期处理"+taskName+"调用服务端开始");
				ckxCFClient.updateUtilControlBiaos();
				logger.info("批量-生效期处理"+taskName+"调用服务端结束");
			}else if("4060".equals(taskName)){
				//看板循环规模计算
				logger.info("批量-看板循环规模计算"+taskName+"调用服务端开始");
				xqjsKbClient.numerationAllUsercenter();
				logger.info("批量-看板循环规模计算"+taskName+"调用服务端结束");
			}else if("4070".equals(taskName)){
				//看板规模自动生效处理
				logger.info("批量-看板规模自动生效处理"+taskName+"调用服务端开始");
				xqjsKbClient.updateXiafgm();
				logger.info("批量-看板规模自动生效处理"+taskName+"调用服务端结束");
			}else if("4080".equals(taskName)){
				//资源快照删除
				logger.info("批量-资源快照删除"+taskName+"调用服务端开始");
				xqjsZykzClient.ziyhqrqManage();
				logger.info("批量-资源快照删除"+taskName+"调用服务端结束");
			}else if("4090".equals(taskName)){
				//周期订单计算（IL日） 
				logger.info("批量-周期订单计算"+taskName+"调用服务端开始");
				xqjsPJClient.pjCalculate();
				logger.info("批量-周期订单计算"+taskName+"调用服务端结束");
			}else if("4100".equals(taskName)){
				//按需订单计算 97W sys
				logger.info("批量-按需订单计算UL"+taskName+"调用服务端开始");				
				xqjsAxClient.anxOrderMethodUL();
				logger.info("批量-按需订单计算UL"+taskName+"调用服务端结束");
			}else if("4101".equals(taskName)){ //hzg 增加 给王凌测试用 2013-5-4
				//按需订单计算 97W sys
				logger.info("批量-按需订单计算UW"+taskName+"调用服务端开始");				
				xqjsAxClient.anxOrderMethodUW();
				logger.info("批量-按需订单计算UW"+taskName+"调用服务端结束");
			}else if("4102".equals(taskName)){ //用户中心VD
				//按需订单计算 97W sys
				logger.info("批量-新按需订单计算VD"+taskName+"调用服务端开始");
				xqjsXaxClient.xinaxOrderMethodVD();				
				logger.info("批量-新按需订单计算VD"+taskName+"调用服务端结束");
			}else if("4103".equals(taskName)){ //用户中心UL
				//新按需订单计算 97W sys
				logger.info("批量-新按需订单计算UL"+taskName+"调用服务端开始");
				xqjsXaxClient.xinaxOrderMethodUL();				
				logger.info("批量-新按需订单计算UL"+taskName+"调用服务端结束");
			}else if("4110".equals(taskName)){
				//KDYS 执行层调度
				logger.info("批量-KDYS"+taskName+"调用服务端开始");
				kdysClient.executeKdysService();
				logger.info("批量-KDYS"+taskName+"调用服务端结束");
			}else if("4120".equals(taskName)){
				logger.info("正在调用批量4120......");
				// 计算累计交付差额
				logger.info("批量-计算累计交付差额"+taskName+"调用服务端开始");
				pclClient.callPcSchedule(null);
				logger.info("批量-计算累计交付差额"+taskName+"调用服务端结束");
			}else if("4130".equals(taskName)){
				// 日滚动排产
				logger.info("批量-日滚动排产"+taskName+"调用服务端开始");
				pcdClient.callPcDailyRoll(null);
				logger.info("批量-日滚动排产"+taskName+"调用服务端结束");
			}else if("4140".equals(taskName)){
				// 滚动周期模拟排产
				logger.info("批量-滚动周期模拟排产"+taskName+"调用服务端开始");
				pcscLient.callPcSchedule(null);
				logger.info("批量-滚动周期模拟排产"+taskName+"调用服务端结束");
			}else if("4150".equals(taskName)){
				logger.info("正在调用批量4150......");
				// 更新入库明细
				logger.info("批量-更新入库明细"+taskName+"调用服务端开始");
				pcpClient.callPcDailyProduce(null);
				logger.info("批量-更新入库明细"+taskName+"调用服务端结束");
			}else if("4160".equals(taskName)){
				// 计算要车计划
				logger.info("批量-计算要车计划"+taskName+"调用服务端开始");
				fjYcjhClient.createYaoHLJhMx();
				logger.info("批量-计算要车计划"+taskName+"调用服务端结束");
			}else if("4170".equals(taskName)){
				// 内部要货令表仓库子仓库批量
				logger.info("批量-内部要货令表仓库子仓库批量"+taskName+"调用服务端开始");
				fjsClient.scheduleRun(null);
				logger.info("批量-内部要货令表仓库子仓库批量"+taskName+"调用服务端结束");
			}else if("4180".equals(taskName)){
				//定时计算cmj
				logger.info("批量-定时计算cmj"+taskName+"调用服务端开始");
				ckxCFClient.calculateCmj();
				logger.info("批量-定时计算cmj"+taskName+"调用服务端结束");
			}else if("4190".equals(taskName)){
				//定时将模板数据导入运输时刻表
				logger.info("批量-定时将模板数据导入运输时刻表"+taskName+"调用服务端开始");
				ckxCFClient.insertTimeOut();
				logger.info("批量-定时将模板数据导入运输时刻表"+taskName+"调用服务端结束");
			}else if("4210".equals(taskName)){
				//运输时刻定时计算方法
				logger.info("批量-输时刻定时计算方法"+taskName+"调用服务端开始");
				ckxCFClient.jisYunssk();
				logger.info("批量-输时刻定时计算方法"+taskName+"调用服务端结束");
			}else if("4220".equals(taskName)){
				//定时检查版次号信息，如果没有与别的表关联则删除
				logger.info("批量-定时检查版次号信息"+taskName+"调用服务端开始");
				ckxCFClient.timingTask();
				logger.info("批量-定时检查版次号信息"+taskName+"调用服务端结束");
			}else if("4230".equals(taskName)){
				logger.info("开始调用批量4230......");
				//清除日历版次(执行层专用)
				logger.info("批量-清除日历版次"+taskName+"调用服务端开始");
				ckxCFClient.clearVersion();
				logger.info("批量-清除日历版次"+taskName+"调用服务端结束");
			}else if("4240".equals(taskName)){
				//将未来编组号按照生效时间更新到编组号中，并清空未来编组号和生效时间(执行层专用)
				logger.info("批量-将未来编组号"+taskName+"调用服务端开始");
				ckxCFClient.updateWeilbzhTobianzh();
				logger.info("批量-将未来编组号"+taskName+"调用服务端结束");
			}else if("4250".equals(taskName)){
				//准备层提醒零件，零件供应商的数量
				logger.info("批量-提醒零件"+taskName+"调用服务端开始");
				xqjsDltxClient.insertCkxShiwtx();
				logger.info("批量-提醒零件"+taskName+"调用服务端结束");
			}else if("4260".equals(taskName)){
				//执行层小火车四个偏移值更新 add hzg 2013.12.13 
				logger.info("批量-执行层小火车四个偏移值更新"+taskName+"调用服务端开始");
				ckxCFClient.autoUpdateXiaohc();
				logger.info("批量-执行层小火车四个偏移值更新"+taskName+"调用服务端结束");
			}else if("4270".equals(taskName)){
				//执行层小火车模板计算 add hzg 2013.12.13 
				logger.info("批量-执行层小火车模板计算"+taskName+"调用服务端开始");
				ckxCFClient.calculateXiaohcmbCk();
				logger.info("批量-执行层小火车模板计算"+taskName+"调用服务端结束");
			}else if("4271".equals(taskName)){
				//执行层小火车模板计算 add zbb 2016.1.11 
				logger.info("批量-执行层小火车模板计算"+taskName+"调用服务端开始");
				ckxCFClient.calculateXXiaohcmbCk();
				logger.info("批量-执行层小火车模板计算"+taskName+"调用服务端结束");
			}else if("4280".equals(taskName)){
				//执行层更新库位包装信息  wangyu add  2014.5.16 
				logger.info("批量-执行层更新库位包装信息"+taskName+"调用服务端开始");
				ckxCFClient.updatekuwbzxx();
				logger.info("批量-执行层更新库位包装信息"+taskName+"调用服务端结束");
			}else if("4290".equals(taskName)){
				//ddbh clv 比较批量计算  add gswang 2014.12.17
				logger.info("批量-ddbh clv 比较批量计算"+taskName+"调用服务端开始");
				xqjsDdbhclvClient.DdbhClvCompare();
				logger.info("批量-ddbh clv 比较批量计算"+taskName+"调用服务端结束");
			}else if("4291".equals(taskName)){
				//ddbh clv 比较批量计算  add gswang 2014.12.17
				logger.info("批量-ddbh clv 比较批量计算"+taskName+"调用服务端开始");
				xqjsJlvXinaxClient.jlvxinaxCompare();
				logger.info("批量-ddbh clv 比较批量计算"+taskName+"调用服务端结束");
			}else if("4300".equals(taskName)){
				//ddbh clv 毛需求拆分  add gswang 2015.12.11
				logger.info("毛需求拆分"+taskName+"调用服务端开始");
				vjmaoxqcfClient.vjMaoxqCaifen();
				logger.info("毛需求拆分"+taskName+"调用服务端结束");
			}else if("4400".equals(taskName)){
				//ddbh clv 毛需求拆分  add gswang 2015.12.11
				logger.info("毛需求拆分"+taskName+"调用服务端开始");
				maoxqcfClient.maoxqCf();
				logger.info("毛需求拆分"+taskName+"调用服务端结束");
			}else if("4311".equals(taskName)){
				//ddbh clv 毛需求拆分  add gswang 2015.12.11
				logger.info("VJ计算Ul"+taskName+"调用服务端开始");
				vJCalculateService.vjCalculateUl();
				logger.info("VJ计算Ul"+taskName+"调用服务端结束");
			}else if("4312".equals(taskName)){
				//ddbh clv 毛需求拆分  add gswang 2015.12.11
				logger.info("VJ计算Uw"+taskName+"调用服务端开始");
				vJCalculateService.vjCalculateUw();
				logger.info("VJ计算Uw"+taskName+"调用服务端结束");
			}else if("4313".equals(taskName)){
				//ddbh clv 毛需求拆分  add gswang 2015.12.11
				logger.info("VJ计算Ux"+taskName+"调用服务端开始");
				vJCalculateService.vjCalculateUx();
				logger.info("VJ计算Ux"+taskName+"调用服务端结束");
			}else if("4314".equals(taskName)){
				//ddbh clv 毛需求拆分  add gswang 2015.12.11
				logger.info("VJ计算VD"+taskName+"调用服务端开始");
				vJCalculateService.vjCalculateVd();
				logger.info("VJ计算VD"+taskName+"调用服务端结束");
			}else if("4330".equals(taskName)){
				//同步分装线排产  add hanwu 2015.12.10
				logger.info("同步分装线排产"+taskName+"调用服务端开始");
				fenzxpcClient.calcFenzxpcjh();
				logger.info("同步分装线排产"+taskName+"调用服务端结束");
			}else if("4321".equals(taskName)){
				logger.info("MJ计算Ul"+taskName+"调用服务端开始");
				mJCalculateService.mjCalculateUl();
				logger.info("MJ计算Ul"+taskName+"调用服务端结束");
			}else if("4322".equals(taskName)){
				//ddbh clv 毛需求拆分  add gswang 2015.12.11
				logger.info("MJ计算Uw"+taskName+"调用服务端开始");
				mJCalculateService.mjCalculateUw();
				logger.info("MJ计算Uw"+taskName+"调用服务端结束");
			}else if("4323".equals(taskName)){
				//ddbh clv 毛需求拆分  add gswang 2015.12.11
				logger.info("MJ计算Ux"+taskName+"调用服务端开始");
				mJCalculateService.mjCalculateUx();
				logger.info("MJ计算Ux"+taskName+"调用服务端结束");
			}else if("4800".equals(taskName)){
				//准备层 取货运费计算  add wangpeng 2016.11.18
				logger.info("取货运费计算"+taskName+"调用服务端开始");
				rukmxService.calcjisyf();
				logger.info("取货运费计算"+taskName+"调用服务端结束");
			}
			
			result = 0;
			logger.info("执行"+taskName+"成功!");
		}catch (Exception e) {
			//result = 1;
			logger.error("批量"+taskName+"异常"+e.getMessage(),e);
		}
		return result;
	}
	
	/**
	 * 调用接口程序
	 * @param taskName
	 * @return
	 */
	private int doInterface(String taskName){
		//定义一个返回值result, 0：表示执行成功，1：表示执行失败,99说明找到文件
		int result = 1;  //将 0 -->1 hzg 2014.5.28
		//定义一个值记录接口编号，进行输出数据源判断
		String codeId = "";
		
		//接口运行开始时间  2013-6-14
		String nowTime = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		
		/**
		 * 接口开始运行  
		 * 	将接口的状态  
		 * 		设置为 1  运行
		 * 		开始时间  设置为 当前时间 
		 * hzg 获取接口的数据源 14.5.9
		 */
		InziDbUtils.getInstance().UpdateInZIDB_ZT("1", baseDao.getSdcDataSource(getSourceId(taskName)).getConnection(), taskName,nowTime);
		logger.info(taskName);		
		int top = 0;
		try {
			//1：解析 enchange-interface.xml 得到此taskName为id的配置信息
			InterfaceConfig inter = InfaceParserConfig.getInstance().getDataParserConfig(taskName);
			if (inter != null) {
				List<OrderConfig> orders = inter.getOrders();
				//执行
				if (inter.getType().equals("in")) {
					//是输入接口  按照输入处理				
					List<OrderConfig> txtOrders = new ArrayList<OrderConfig>(); //处理txt的配置
					List<OrderConfig> tableOrders = new ArrayList<OrderConfig>(); //向业务表分发的配置

					for (OrderConfig order : orders) {
						if ("true".equals(order.getIstxt())|| order.getIstxt() == null) {
							//为true  或者  不填  则为处理txt
							txtOrders.add(order);
						} else {
							tableOrders.add(order);
						}
					}

					//1: txt--->中间表或业务表 多线程处理（TXT->DB）
					for (int i = 0; i < txtOrders.size(); i++) {
						codeId = txtOrders.get(i).getId();
						try{
							dataEchange.doExchange(txtOrders.get(i).getId(),dataEchange.IN_INTERFACE);
						}catch (FileNotFindException e1){
							throw e1;
						}catch(Exception e){
							if(!"sppv".equals(txtOrders.get(i).getSort())){
								throw e;
							}else{//给集中监控平台发送消息 hzg 2014.9.15
								logger.error("SPPV接口:"+taskName+"出现异常，报集中监控平台处理......"+e.getMessage(),e);
								sendSppvAlarm(taskName,e);
							}
						}
					}

					//2; 中间表--->业务表（DB->DB）  多线程
					if (top <= 0) {
						//才执行分发					
						for (int j = 1; j <= tableOrders.size(); j++) {
							//设置文件移动的标记  为true  则移动文件  .  执行最后一个order后，移动 
							dataEchange.doExchange(tableOrders.get(j - 1).getId(),dataEchange.IN_INTERFACE);
						}
					}

				}else if(inter.getType().equals("dout")){
					djkWriter.write(inter.getId());
				}else {
					//或则是out 按照输出处理
					//现在默认 是一个接着一个 默认执行
					for (OrderConfig orderCon : orders) {
						codeId = orderCon.getId();
						if(orderCon.getIsDb() != null && "true".equals(orderCon.getIsDb())){
							dataEchange.doExchange(orderCon.getId(),dataEchange.IN_INTERFACE);
						}else{
							dataEchange.doExchange(orderCon.getId(),dataEchange.OUT_INTERFACE);
						}
						
					}
				}
			}
		}catch(FileNotFindException e){
			/**
			 * 接口结束或者发生异常
			 * 	将接口状态更改为结束
			 * 	结束时间 维护为当前时间
			 */
			top=1;
			baseDao.getSdcDataSource(getSourceId(codeId));
			InziDbUtils.getInstance().UpdateInZIDB_ZT(RUN_ERROR_STATE, baseDao.getConnection(), taskName,nowTime);
			dataEchange.total=0;
			logger.error("没有找到文件,状态99");
			return NOT_FOUND_FILE;
		}catch (Exception ex){
			//接口直接异常 总表状态设置为 运行出错
			/**
			 * 接口结束或者发生异常
			 * 	将接口状态更改为出错
			 * 	结束时间 维护为当前时间
			 */
			top=1;
			logger.error("出现异常,开始更新接口字典表状态为2"+ex.getMessage());
			try{
				InziDbUtils.getInstance().UpdateInZIDB_ZT(RUN_ERROR_STATE, baseDao.getSdcDataSource(getSourceId(codeId)).getConnection(), taskName,nowTime);
			}catch(Exception e1){
				logger.error("接口:"+taskName+"更新字典表异常"+e1.getMessage(),e1);
			}
			dataEchange.total=0;
			logger.error(ex.getMessage(),ex);
			logger.error("程序出现异常,返回状态1");
			return EXP_ERROR;
		}
		
		//没有出错 则 向接口总表中  将状态设置为 完成 
		/**
		 * 接口结束或者发生异常
		 * 	将接口状态更改为结束
		 * 	结束时间 维护为当前时间
		 */
		if(top<=0){
			//根据接口总表的ID和开始时间，结合'接口文件错误记录信息'表  如果有记录 则认为此接口运行是有异常的
			result = InziDbUtils.getInstance().UpdateInZIDB_ZTByMx(nowTime, baseDao.getSdcDataSource(getSourceId(codeId)).getConnection(), taskName); 
			logger.info("当前接口:"+taskName+",状态为："+result);
		}
		dataEchange.total=0;
		logger.info("当前接口:"+taskName+",状态为："+result);
		return result;
	}
	/**
	 * 批量执行接口命令
	 */
	@Override
	public void dispatchTasks(List<String> taskNames) {	
		if(taskNames!=null){
			for(String taskName : taskNames){
				dispatchTask(taskName);
			}
		}
	}
	
	 /**
	 * 接口数据源取值
	 */
	public String getSourceId(String codeId){
		String sourceId = codeId.startsWith("3")?"2":"1";
		return sourceId;
	}
	
	
	/**
	 * 判断是否是sppv异常，如果是则向集中监控平台发送报警
	 * @author 贺志国
	 * @date 2014-9-15
	 * @param taskName
	 * @param e
	 */
	public void sendSppvAlarm(String taskName,Exception e){
		//发送报警 1.数据库异常
		try {
			String []args = e.getMessage().split("@"); 
			String sppvId = args[0].substring(args[0].length()-1,args[0].length());
			if("4".equals(sppvId)){
				sppvId="sppv1";
			}else if("6".equals(sppvId)){
				sppvId="sppv2";
			}
			SysmonitorService service = new SysmonitorService();
			if(e.getMessage().contains("SQL")||e.getMessage().contains("JDBC")){
				service.SendAlert("", "0005", "3", "007", taskName+" Database connection failed. "+sppvId);
				logger.info("####"+e.getMessage());
			}else{//2.业务处理异常
				service.SendAlert("", "0003", "3", "007", taskName+" Business operate error. "+sppvId);
				logger.info("@@@@"+e.getMessage());
			}
		}catch (Exception e1) {
			  logger.error(taskName+"==>>发送报警出错：" + e1.getMessage());
		}
	}
	
	

}
