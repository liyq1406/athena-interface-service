/**
 * 
 */
package com.athena.component.service.imp;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jws.WebService;

import org.apache.log4j.Logger;

import com.athena.component.service.BaseService;
import com.athena.component.service.IKdysService;
import com.athena.component.service.bean.Inwuld;
import com.athena.component.service.bean.Jizxgl;
import com.athena.component.service.bean.TC;
import com.athena.component.service.bean.Waibwlxx;
import com.athena.component.service.bean.Wuld;
import com.athena.component.service.bean.Yaohl;
import com.athena.db.ConstantDbCode;
import com.athena.util.athenalog.impl.UserOperLog;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;
import com.toft.core3.container.annotation.Component;
import com.toft.core3.container.annotation.Inject;
import com.toft.core3.transaction.annotation.Transactional;

/**
 * @author dsimedd001
 * 
 */
@WebService(endpointInterface="com.athena.ck.module.kdys.service.IKdysService", serviceName="/kdysService")
@Component
public class KdysService extends BaseService implements IKdysService{
	// log4j日志初始化
	static Logger logger = Logger.getLogger(KdysService.class); 
	@Inject
	private UserOperLog userOperLog;
	/**
	 * 获取Kdys更新信息 逻辑： KDYS更新后，将数据表更新进入In_wuld表中，将其发布成服务后，自动的读取In_wuld中的数据
	 * 
	 * @return
	 */
	public String executeKdysService() throws ServiceException{
		logger.info("××××××××××××接口4110运行开始×××××××××××××");
		//查询处理状态为零的接口数据（处理状态：0、未处理，1、已处理）
		//首先对处理状态为 0 的 更新为中间状态 2  然后在对状态为2 的进行处理，处理完成后在更新为最终状态 1  防止并发。
		baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.updatezjInwuld");
		Inwuld bean = new Inwuld();
		bean.setClzt("2");
		List<Inwuld> listInwuld = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryInwuld", bean);
		
		// 初始化标识为A的List集合（插入数据）
		List<Inwuld> insertTcList = new ArrayList<Inwuld>();
		// 初始化标识为B的List集合
		List<Inwuld> updateTcList = new ArrayList<Inwuld>();
		// 初始化标识为C的List集合（更新物理点以及要货令数据）
		List<Inwuld> updateTcWuldList = new ArrayList<Inwuld>();
		for(Inwuld inwuld : listInwuld){
			/**
			 * 1、如果标识为A，则将数据加入集合insertTcList中 2、标识为B，则将数据加入集合updateTcList中
			 * 3、标识为C，则将数据加入集合updateTcWuldList中
			 */
			if (inwuld.getFalg() != null && "A".equals(inwuld.getFalg())) {
				insertTcList.add(inwuld);
			} else if (inwuld.getFalg() != null && "B".equals(inwuld.getFalg())) {
				updateTcList.add(inwuld);
			} else if (inwuld.getFalg() != null && "C".equals(inwuld.getFalg())) {
				updateTcWuldList.add(inwuld);
			}
			//设置处理状态  加一个中间状态 为 2
			//inwuld.setClzt("2");
		}
		try {
		// 插入TC信息
		logger.info("接口4110批量新增标识A(insertTC)开始");
		this.insertTC(insertTcList);
		logger.info("接口4110批量新增标识A(insertTC)结束");
		// 更新TC信息
		logger.info("接口4110批量更新B标识(updateTC)开始");
		this.updateTC(updateTcList);
		logger.info("接口4110批量更新B标识(updateTC)结束");
		// 更新TC物理点信息
		logger.info("接口4110批量更新C标识(物理点以及要货令数据)开始");
		this.updateTCWuld(updateTcWuldList);
		logger.info("接口4110批量更新C标识(物理点以及要货令数据)结束");
		//更新处理状态  将中间状态为 2 的 更新为 1
			baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).executeBatch("kdysService.updateInwuld", listInwuld);
		logger.info("××××××××××××接口4110运行结束×××××××××××××");
		} catch (Exception e) {
			logger.error("接口4110批量更新处理状态数据报错"+e.getMessage());
		}
		return "success";
	}

	/**
	 * 获取kdys更新信息 业务标识为A： 根据"KDYS提单号"和"KDYS集装箱号"两个维度存在则将现有文本插入数据表中
	 * 
	 * @param insertTcList
	 * @throws ParseException 
	 */
	@Transactional
	private void insertTC(List<Inwuld> insertTcList) throws ParseException {
		String createTime = DateUtil.curDateTime();
		String createTimeNoLine = DateUtil.StringFormatToString(createTime);
		String currentId = this.getId(createTimeNoLine);
//		List<TC> listInsertTc = new ArrayList<TC>();
//		List<Jizxgl> listDeleteJizxgl = new ArrayList<Jizxgl>();
//		List<Jizxgl> listInsertJizxgl = new ArrayList<Jizxgl>();
		try{
			for (Inwuld inwuld : insertTcList) {
				TC tc = new TC();
				// TC号
				tc.setTcNo(inwuld.getKdysBoxId());			
				// kdys提单号
				tc.setKdysSheetId(inwuld.getKdysSheetId());
				// 看TC信息是否存在,如果不存在则进行新增
				List<TC> listTC = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryTcZT", tc);
				if (listTC.size() < 1) {
					//判断对应的要货令是否存在。
					List<Yaohl> yaohlList = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryYaohl", tc);
					if (yaohlList.size() > 0){
						//1、插入TC数据 
						String currentUth = "";
						for(Yaohl yaohl : yaohlList){
							if (!currentUth.equals(yaohl.getUth())){
								//设置值
								currentUth = yaohl.getUth();
								//获取TC数据
								tc = this.getTc(inwuld, yaohl);
								//Id号
								currentId = String.format("%04d",  Integer.valueOf(currentId) + 1);
								tc.setId(createTimeNoLine + currentId);
								//创建人
								tc.setCreator("4110");
								//创建时间
								tc.setCreateTime(createTime);
								//修改人
								tc.setEditor("4110");
								//修改时间
								tc.setEditTime(createTime);
								//插入TC数据
								baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.insertTC", tc);
								//listInsertTc.add(tc);
							}
						}
						//2、更新集装箱管理
						Jizxgl jizxgl = new Jizxgl();
						jizxgl.setJizxh(tc.getTcNo());
						jizxgl.setUsercenter(yaohlList.get(0).getUsercenter());
						jizxgl = (Jizxgl)baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).selectObject("kdysService.queryJizxgl", jizxgl);
						//判断集装箱是否存在并且状态为1、重箱或2、空箱
						if (jizxgl != null){
							//如果是0、待申报    3、返箱 先删除数据，然后再添加
							if ("0".equals(jizxgl.getZhuangt()) || "3".equals(jizxgl.getZhuangt())){
								//删除数据
								baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.deleteJizxgl",jizxgl );
								//listDeleteJizxgl.add(jizxgl);
								//添加数据
								jizxgl = this.getJizxgl(tc ,yaohlList.get(0));
								jizxgl.setCreator("4110");
								jizxgl.setCreate_time(createTime);
								baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.insertJizxgl", jizxgl);
								//listInsertJizxgl.add(jizxgl);
							}
						} else {
							//添加数据
							jizxgl = this.getJizxgl(tc ,yaohlList.get(0));
							jizxgl.setCreator("4110");
							jizxgl.setCreate_time(createTime);
							baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.insertJizxgl", jizxgl);
							//listInsertJizxgl.add(jizxgl);
						}
					}
				}
			}
		}catch(RuntimeException e){
			logger.error("接口4110批量新增标识A(insertTC)数据报错"+e.getMessage());
		}
		//删除集装箱
		//baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).executeBatch("kdysService.deleteJizxgl", listDeleteJizxgl);
		//添加集装箱
		//baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).executeBatch("kdysService.insertJizxgl", listInsertJizxgl);
		//添加Tc表 
		//baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).executeBatch("kdysService.insertTC", listInsertTc);
	}

	/**
	 * 业务标识为B： 根据"PAP集装箱号"和"pap提单号"两个维度,更新"KDYS集装箱号"
	 * 
	 * @param updateTcList
	 * @throws ParseException 
	 */
	@Transactional
	private void updateTC(List<Inwuld> updateTcList) throws ParseException {
		String editTime = DateUtil.curDateTime();
		String editTimeNoLine = DateUtil.StringFormatToString(editTime);
		String currentId = this.getId(editTimeNoLine);
		List<TC> listDeleteTc = new ArrayList<TC>();
		List<TC> listInsertTc = new ArrayList<TC>();
//		List<Jizxgl> listDeleteJizxgl = new ArrayList<Jizxgl>();
//		List<Jizxgl> listInsertJizxgl = new ArrayList<Jizxgl>();
//		List<Yaohl> listUpdateYaohl = new ArrayList<Yaohl>();
		try{
			for (Inwuld inwuld : updateTcList) {
				//如果集装箱号变了  那么需要按新的逻辑处理。如果只是  提单号变了 那么直接根据"PAP集装箱号"和"pap提单号"两个维度,更新"KDYS集装箱号"
				if(!inwuld.getPapBoxId().equals(inwuld.getKdysBoxId())){
					// 初始化TC
					// pap集装箱号
					//newTc.setTcNo(inwuld.getPapBoxId());
					// kdys提单号
					//newTc.setKdysSheetId(inwuld.getKdysSheetId());
					TC oldTc = new TC();
					oldTc.setTcNo(inwuld.getPapBoxId());
					oldTc.setKdysSheetId(inwuld.getPapSheetId());
					// 看TC信息是否存在,如果存在，则更新
					List<TC> listTC = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryTcZT", oldTc);
					//if (listTC.size() > 0) {
						//if (oldYaohlList.size()>0){
							//1、更新原始TC数据 为 空箱
							if(listTC.size() > 0){
								for(TC tc : listTC){
									//修改人
									tc.setEditor("4110");
									//修改时间
									tc.setEditTime(editTime);
									baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.deleteTc1", tc);
									//listDeleteTc.add(tc);
								}
							}
							//查询原始的要货令数据 
							//TC oldTc = new TC();
							List<Yaohl> oldYaohlList = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryYaohl", oldTc);
							if (oldYaohlList.size()>0){
								//2、还原要货令数据
								for (Yaohl yaohl : oldYaohlList){
									if (yaohl.getYaohlh() !=null && !"".endsWith(yaohl.getYaohlh())){
										yaohl.setXiughyjjfsj("");
										yaohl.setJiaofzt("");
										yaohl.setShijfysj("");
										baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.updateYaohl", yaohl);
										//listUpdateYaohl.add(yaohl);
									}
								}
								//3、删除集装箱数据
								Jizxgl oldJizxgl = new Jizxgl();
								oldJizxgl.setJizxh(oldTc.getTcNo());
								oldJizxgl.setUsercenter(oldYaohlList.get(0).getUsercenter());
								oldJizxgl =(Jizxgl)baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).selectObject("kdysService.queryJizxgl", oldJizxgl);
								if (oldJizxgl!=null && "0".equals(oldJizxgl.getZhuangt())){
									baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.deleteJizxgl", oldJizxgl);
									//listDeleteJizxgl.add(oldJizxgl);
									}
							}
							//4、查询新的要货令数据 
							//查询最新的要货令数据
							TC newTc = new TC();
							newTc.setTcNo(inwuld.getKdysBoxId());
							newTc.setKdysSheetId(inwuld.getKdysSheetId());
							List<Yaohl> newYaohlList = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryYaohl", newTc);
							if (newYaohlList.size() > 0){  //存在要货令数据
								//1、更新TC数据  即更新他的 提单号 和 集装箱号
								String currentUth = "";
								for(Yaohl yaohl : newYaohlList){
									if (!currentUth.equals(yaohl.getUth())){
										TC tc = this.getTc(inwuld, yaohl);
										// pap集装箱号
										tc.setPapBoxId(inwuld.getKdysBoxId());
										// pap提单号
										tc.setPapSheetId(inwuld.getKdysSheetId());
										//Id号
										currentId = String.format("%04d",  Integer.valueOf(currentId) + 1);
										tc.setId(editTimeNoLine + currentId);
										//创建人
										tc.setCreator("4110");
										//创建时间
										tc.setCreateTime(editTime);
										//修改人
										tc.setEditor("4110");
										//修改时间
										tc.setEditTime(editTime);
										//插入TC数据
										listInsertTc.add(tc);
										//baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.updateTC1", tc);
										//设置值
										currentUth = yaohl.getUth();
									}
								}
								//2、更新集装箱管理
								Jizxgl jizxgl = new Jizxgl();
								jizxgl.setJizxh(newTc.getTcNo());
								jizxgl.setUsercenter(newYaohlList.get(0).getUsercenter());
								jizxgl = (Jizxgl)baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).selectObject("kdysService.queryJizxgl", jizxgl);
								//判断集装箱是否存在并且状态为1、重箱或2、空箱
								if (jizxgl != null){
									//如果是0、待申报    3、返箱 先删除数据，然后再添加
									if ("0".equals(jizxgl.getZhuangt()) || "3".equals(jizxgl.getZhuangt())){
										//删除数据
										baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.deleteJizxgl", jizxgl);
										//listDeleteJizxgl.add(jizxgl);
										//添加数据
										jizxgl = this.getJizxgl(newTc ,newYaohlList.get(0));
										jizxgl.setCreator("4110");
										jizxgl.setCreate_time(editTime);
										baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.insertJizxgl", jizxgl);
										//listInsertJizxgl.add(jizxgl);
									}
								} else {
									//添加数据
									jizxgl = this.getJizxgl(newTc ,newYaohlList.get(0));
									jizxgl.setCreator("4110");
									jizxgl.setCreate_time(editTime);
									baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.insertJizxgl", jizxgl);
									//listInsertJizxgl.add(jizxgl);
								}
							}
				}else{
					//直接根据"PAP集装箱号"和"pap提单号"两个维度,更新"KDYS集装箱号"
					// 初始化TC
					TC newTc = new TC();
					// KDYS集装箱号
					newTc.setTcNo(inwuld.getKdysBoxId());
					// 看TC信息是否存在,如果存在，则更新 集装箱号
					List<TC> listTC = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryTcZT", newTc);
					for(TC tc1 : listTC){
						//更新集装箱号
						// pap集装箱号
						tc1.setPapBoxId(inwuld.getPapBoxId());
						// pap提单号
						tc1.setPapSheetId(inwuld.getPapSheetId());
						// kdys提单号
						tc1.setKdysSheetId(inwuld.getKdysSheetId());
						//修改人
						tc1.setEditor("4110");
						//修改时间
						tc1.setEditTime(editTime);
						baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.updateJzxh", tc1);
					}
				}
					//}
				//}
			}
			
			//批量更新要货令
			//baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).executeBatch("kdysService.updateYaohl", listUpdateYaohl);
			//批量删除集装箱
			//baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).executeBatch("kdysService.deleteJizxgl", listDeleteJizxgl);
			//批量添加集装箱
			//baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).executeBatch("kdysService.insertJizxgl", listInsertJizxgl);
			//批量更新TC表
			//baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).executeBatch("kdysService.deleteTc1", listDeleteTc);
			//批量添加Tc表 
			baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).executeBatch("kdysService.insertTC", listInsertTc);
		}catch(RuntimeException e){
			logger.error("接口4110批量更新B标识(updateTC)数据报错"+e.getMessage());
			throw new ServiceException("接口4110批量更新updateTC数据报错"+e.getMessage());
		}
	}

	/**
	 * 业务标识为C： 根据"PAP提单号"和"PAP集装箱号"两个维度更新"时间"和"物理点"两个维度 注意点：
	 * 如果C标识的信息在没有A的情况下，则将其信息先进行插入，再将其信息进行更新
	 * 
	 * @param updateTcWuldList
	 */
	@Transactional
	private void updateTCWuld(List<Inwuld> updateTcWuldList)throws ParseException {
		//查询物理点数据
		List<Wuld> listWuld = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryWuld");
		//查询外部物流信息数据
		List<Waibwlxx> listWaibwlxx = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryWaibwlxx");
		String createTime = DateUtil.curDateTime();
		String createTimeNoLine = DateUtil.StringFormatToString(createTime);
		String currentId = this.getId(createTimeNoLine);
		List<TC> listUpdateTc = new ArrayList<TC>();
		try{
			for (Inwuld inwuld : updateTcWuldList) {
				TC tc = new TC();
				// TC号
				tc.setTcNo(inwuld.getKdysBoxId());			
				// kdys提单号
				tc.setKdysSheetId(inwuld.getKdysSheetId());
				tc.setZuiswld(inwuld.getcPointId());			// 最新物理点
				//修改 物理点顺序号在700以后的，如果KDYS还有传数据过来的话，根据 TC状态去判断  如果状态为重箱的话，只需要更新即可，如果申报后变空箱，则直接跳过。
				Wuld wuld = (Wuld)baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).selectObject("kdysService.queryWuldC",inwuld.getcPointId());
					if(null != wuld && wuld.getShunxh().compareTo("700") >= 0){
						//先根据箱子号去查询 不带状态
						List<TC> listTCZt = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryTc", tc);
						for (TC tczt : listTCZt){
							//如果箱子状态为 0 表示已经变空箱  则不作任何处理
							if("0".equals(tczt.getTczt())){
								continue;
							}
							//否则还是做更新处理
							//查询要货令数据
							List<Yaohl> yaohlList = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryYaohl", tc);
							if (yaohlList.size() > 0){
								//获取起运点或起运时间
								Wuld currentWuld = new Wuld();
								for (Wuld wld : listWuld){
									if (wld.getWuldbh().equals(inwuld.getcPointId())){
										currentWuld = wld;
										break;
									}
								}
								///吴易超2014/2/11 对以下代码进行注释
								if (currentWuld == null){			//不存在原始物理点，不更新
									String errInfo = "KDYS提单号：" + inwuld.getKdysSheetId() + "-----KDYS集装箱号：" + inwuld.getKdysBoxId() + "对应的物理点("+inwuld.getcPointId()+")数据在参考系中未维护，不能更新";
									userOperLog.addError("CK", "KDYS", "KDYS", "KdysService", errInfo);
									logger.error(errInfo);
									continue;
								}
								///吴易超2014/2/11 对以上代码进行注释
								//计算最新预计到达时间
								Waibwlxx currentWaibwlxx = new Waibwlxx();
								for(Waibwlxx waibwlxx : listWaibwlxx){
									if (waibwlxx.getUsercenter() != null && !"".equals(waibwlxx.getUsercenter())
										&&waibwlxx.getLujbh() != null && !"".equals(waibwlxx.getLujbh())
										&&waibwlxx.getWuldbh() != null && !"".endsWith(waibwlxx.getWuldbh())){
										if (waibwlxx.getUsercenter().equals(yaohlList.get(0).getUsercenter())
												&&waibwlxx.getLujbh().equals(yaohlList.get(0).getLujdm())
												&&waibwlxx.getWuldbh().equals(tc.getZuiswld())){
												//（用户中心，路径编号，物理点相同）
												currentWaibwlxx = waibwlxx;
												break;
											}
									}
								}
								///吴易超2014/2/11 对以下代码进行注释
								if (currentWaibwlxx == null){			//不存在外部物流详细，不更新
									String errInfo = "KDYS提单号：" + inwuld.getKdysSheetId() + "-----KDYS集装箱号：" + inwuld.getKdysBoxId() + "无对应的外部物流信息，不能更新";
									userOperLog.addError("CK", "KDYS", "KDYS", "KdysService", errInfo);
									logger.error(errInfo);
									continue;
								}
								///吴易超2014/2/11 对以上代码进行注释
								//获取TC
								tc = this.getTCForWuld(inwuld, currentWuld, currentWaibwlxx);
								//获取要货令
								yaohlList = this.getYaohl(yaohlList, tc);
								//循环更新要货令
								for(Yaohl yaohl : yaohlList){
									baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.updateYaohl", yaohl);
								}
								//baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).executeBatch("kdysService.updateYaohl", yaohlList);
								listUpdateTc.add(tc);
							}
						}
					}else{
						if (null == wuld) {
							String errInfo = "KDYS提单号：" + inwuld.getKdysSheetId() + "-----KDYS集装箱号：" + inwuld.getKdysBoxId() + "对应的物理点("+inwuld.getcPointId()+")数据在参考系（CKX_YUNSWLD）中未维护，不能更新";
							userOperLog.addError("CK", "KDYS", "KDYS", "KdysService", errInfo);
							logger.error(errInfo);
						}
						//在次判断是否有漏掉的集装箱      看TC信息是否存在,如果不存在则进行新增
						//修改为 根据 TC号  和 箱子状态来判断是否存在  存在就更新  不存在的话 在新增
						List<TC> listTC = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryTcZT", tc);
						if(listTC.size()<1){
							//判断对应的要货令是否存在。
							List<Yaohl> yaohlList = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryYaohl", tc);
							if (yaohlList.size() > 0){
								//获取启运港口和关箱时间
								Wuld insertWuld = new Wuld();
								for (Wuld wld : listWuld){
									if (wld.getWuldbh().equals(inwuld.getcPointId())){
										insertWuld = wld;
										break;
									}
								}
								if (insertWuld == null){			//不存在原始物理点，不插入
									String errInfo = "KDYS提单号：" + inwuld.getKdysSheetId() + "-----KDYS集装箱号：" + inwuld.getKdysBoxId() + "对应的物理点("+inwuld.getcPointId()+")数据在参考系中未维护，不能插入";
									userOperLog.addError("CK", "KDYS", "KDYS", "KdysService", errInfo);
									logger.error(errInfo);
									continue;
								}
								//1、插入TC数据 
								String currentUth = "";
								for(Yaohl yaohl : yaohlList){
									if (!currentUth.equals(yaohl.getUth())){
										//设置值
										currentUth = yaohl.getUth();
										//获取TC数据
										tc = this.getTcByC(inwuld,insertWuld, yaohl);
										//Id号
										currentId = String.format("%04d",  Integer.valueOf(currentId) + 1);
										tc.setId(createTimeNoLine + currentId);
										//创建人
										tc.setCreator("4110");
										//创建时间
										tc.setCreateTime(createTime);
										//修改人
										tc.setEditor("4110");
										//修改时间
										tc.setEditTime(createTime);
										//插入TC数据
										baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.insertTCC1", tc);
										//listInsertTc.add(tc);
									}
								}
								//2、更新集装箱管理
								Jizxgl jizxgl = new Jizxgl();
								jizxgl.setJizxh(tc.getTcNo());
								jizxgl.setUsercenter(yaohlList.get(0).getUsercenter());
								jizxgl = (Jizxgl)baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).selectObject("kdysService.queryJizxgl", jizxgl);
								//判断集装箱是否存在并且状态为1、重箱或2、空箱
								if (jizxgl != null){
									//如果是0、待申报    3、返箱 先删除数据，然后再添加
									if ("0".equals(jizxgl.getZhuangt()) || "3".equals(jizxgl.getZhuangt())){
										//删除数据
										baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.deleteJizxgl",jizxgl );
										//listDeleteJizxgl.add(jizxgl);
										//添加数据
										jizxgl = this.getJizxgl(tc ,yaohlList.get(0));
										jizxgl.setCreator("4110");
										jizxgl.setCreate_time(createTime);
										baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.insertJizxgl", jizxgl);
										//listInsertJizxgl.add(jizxgl);
									}
								} else {
									//添加数据
									jizxgl = this.getJizxgl(tc ,yaohlList.get(0));
									jizxgl.setCreator("4110");
									jizxgl.setCreate_time(createTime);
									baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.insertJizxgl", jizxgl);
									//listInsertJizxgl.add(jizxgl);
								}
							}
						}
						
							//查询要货令数据
							List<Yaohl> yaohlList = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryYaohl", tc);
							if (yaohlList.size() > 0){
								//获取起运点或起运时间
								Wuld currentWuld = new Wuld();
								for (Wuld wld : listWuld){
									if (wld.getWuldbh().equals(inwuld.getcPointId())){
										currentWuld = wld;
										break;
									}
								}
								///吴易超2014/2/11 对以下代码进行注释
								if (currentWuld == null){			//不存在原始物理点，不更新
									String errInfo = "KDYS提单号：" + inwuld.getKdysSheetId() + "-----KDYS集装箱号：" + inwuld.getKdysBoxId() + "对应的物理点("+inwuld.getcPointId()+")数据在参考系中未维护，不能更新";
									userOperLog.addError("CK", "KDYS", "KDYS", "KdysService", errInfo);
									logger.error(errInfo);
									continue;
								}
								///吴易超2014/2/11 对以上代码进行注释
								//计算最新预计到达时间
								Waibwlxx currentWaibwlxx = new Waibwlxx();
								for(Waibwlxx waibwlxx : listWaibwlxx){
									if (waibwlxx.getUsercenter() != null && !"".equals(waibwlxx.getUsercenter())
										&&waibwlxx.getLujbh() != null && !"".equals(waibwlxx.getLujbh())
										&&waibwlxx.getWuldbh() != null && !"".endsWith(waibwlxx.getWuldbh())){
										if (waibwlxx.getUsercenter().equals(yaohlList.get(0).getUsercenter())
												&&waibwlxx.getLujbh().equals(yaohlList.get(0).getLujdm())
												&&waibwlxx.getWuldbh().equals(tc.getZuiswld())){
												//（用户中心，路径编号，物理点相同）
												currentWaibwlxx = waibwlxx;
												break;
											}
									}
								}
								///吴易超2014/2/11 对以下代码进行注释
								if (currentWaibwlxx == null){			//不存在外部物流详细，不更新
									String errInfo = "KDYS提单号：" + inwuld.getKdysSheetId() + "-----KDYS集装箱号：" + inwuld.getKdysBoxId() + "无对应的外部物流信息，不能更新";
									userOperLog.addError("CK", "KDYS", "KDYS", "KdysService", errInfo);
									logger.error(errInfo);
									continue;
								}
								///吴易超2014/2/11 对以上代码进行注释
								//当物理点为30(其他的物理点不考虑)，箱子存在的时候，在次判断是否有漏掉的UT 如果有 则需要将漏掉的补进去
								if("30".equals(inwuld.getcPointId())){
									//获取启运港口和关箱时间
									Wuld Wuld30 = new Wuld();
									for (Wuld wld : listWuld){
										if (wld.getWuldbh().equals(inwuld.getcPointId())){
											Wuld30 = wld;
											break;
										}
									}
									if (Wuld30 == null){			//不存在原始物理点，不插入
										String errInfo = "KDYS提单号：" + inwuld.getKdysSheetId() + "-----KDYS集装箱号：" + inwuld.getKdysBoxId() + "对应的物理点("+inwuld.getcPointId()+")数据在参考系中未维护，不能插入";
										userOperLog.addError("CK", "KDYS", "KDYS", "KdysService", errInfo);
										logger.error(errInfo);
										continue;
									}
									//1、插入漏掉的UT对应的TC数据 
									List<Yaohl> UTList = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryYaohlByut", tc); 
									for(Yaohl yaohl : UTList){
										//在根据箱子和UT号来查询数据是否存在 如果存在，则继续下一个
										TC tc1 = new TC();
										tc1.setTcNo(inwuld.getKdysBoxId());	
										tc1.setUtNo(yaohl.getUth());
										List<TC> listTCByUt = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryTcbyUt", tc1);
										if(listTCByUt.size()!=0){
											continue;
										}
										//如果不存在，则属于漏掉的UT号  则需要补进去
										//1、插入TC数据 
										String Uth = "";
										//在次查询该UT号对应的要货令
										List<Yaohl> yaohlListByUt = baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).select("kdysService.queryYaohlUt", tc1);
										for(Yaohl yaohlbyut : yaohlListByUt){
											if (!Uth.equals(yaohlbyut.getUth())){
												//设置值
												Uth = yaohlbyut.getUth();
												//获取TC数据
												tc = this.getTcByUT(inwuld,Wuld30, yaohlbyut);
												//Id号
												currentId = String.format("%04d",  Integer.valueOf(currentId) + 1);
												tc.setId(createTimeNoLine + currentId);
												//创建人
												tc.setCreator("4110");
												//创建时间
												tc.setCreateTime(createTime);
												//修改人
												tc.setEditor("4110");
												//修改时间
												tc.setEditTime(createTime);
												//插入TC数据
												baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.insertTCC1", tc);
												//listInsertTc.add(tc);
											}
										  }
									   }
									}else{
										//获取TC
										tc = this.getTCForWuld(inwuld, currentWuld, currentWaibwlxx);
										//获取要货令
										yaohlList = this.getYaohl(yaohlList, tc);
										//循环更新要货令
										for(Yaohl yaohl : yaohlList){
											baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).execute("kdysService.updateYaohl", yaohl);
										}
										//baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).executeBatch("kdysService.updateYaohl", yaohlList);
										listUpdateTc.add(tc);
									}
							 }
					}
					//更新TC信息
					baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).executeBatch("kdysService.updateTcInfo", listUpdateTc);
			}
	    }catch(RuntimeException e){
	    	logger.error("接口4110批量更新C标识(物理点以及要货令数据)数据报错"+e.getMessage());
	    }
	}
	/*
	 * 获取TC数据
	 */
	private TC getTc(Inwuld inwuld,Yaohl yaohl){
		TC tc = new TC();
		// TC号
		tc.setTcNo(inwuld.getKdysBoxId());
		// kyds提单号
		tc.setKdysSheetId(inwuld.getKdysSheetId());
		// pap集装箱号
		tc.setPapBoxId(inwuld.getPapBoxId());
		// pap提单号
		tc.setPapSheetId(inwuld.getKdysSheetId());
		// ut号
		tc.setUtNo(yaohl.getUth());		
		// 目的地
		tc.setMudd(yaohl.getCangkbh());		
		// 订货车间
		tc.setDinghcj(yaohl.getDinghcj());		
		// 预计到达时间
		tc.setYujddsj(yaohl.getZuiwsj());	
		// 路径代码
		tc.setLujdm(yaohl.getLujdm());	
		// 状态
		tc.setTczt("1");
		//新增 箱型
		tc.setXiangx(inwuld.getXiangx());
		return tc;
	}
	
	/*
	 * 获取TC数据
	 */
	private TC getTcByC(Inwuld inwuld,Wuld wuld, Yaohl yaohl){
		TC tc = new TC();
		// TC号
		tc.setTcNo(inwuld.getKdysBoxId());
		// kyds提单号
		tc.setKdysSheetId(inwuld.getKdysSheetId());
		// pap集装箱号
		tc.setPapBoxId(inwuld.getPapBoxId());
		// pap提单号
		tc.setPapSheetId(inwuld.getKdysSheetId());
		// ut号
		tc.setUtNo(yaohl.getUth());		
		// 目的地
		tc.setMudd(yaohl.getCangkbh());		
		// 订货车间
		tc.setDinghcj(yaohl.getDinghcj());		
		// 预计到达时间
		tc.setYujddsj(yaohl.getZuiwsj());	
		// 路径代码
		tc.setLujdm(yaohl.getLujdm());	
		// 状态
		tc.setTczt("1");
		//新增 箱型
		tc.setXiangx(inwuld.getXiangx());
		if (wuld.getShunxh() != null){
			//新增  物理点顺序号 在200 的时候  更新  只更新一次  获取起运点和关箱时间
			if (wuld.getShunxh().compareTo("100") >= 0 && wuld.getShunxh().compareTo("199") < 0){
				tc.setGuanxsj(inwuld.gettTime());
				tc.setQiyd(inwuld.getcPointId());
			}
		}
		return tc;
	}
	
	/*
	 * 获取TC数据
	 */
	private TC getTcByUT(Inwuld inwuld,Wuld wuld, Yaohl yaohl){
		TC tc = new TC();
		// TC号
		tc.setTcNo(inwuld.getKdysBoxId());
		// kyds提单号
		tc.setKdysSheetId(inwuld.getKdysSheetId());
		// pap集装箱号
		tc.setPapBoxId(inwuld.getPapBoxId());
		// pap提单号
		tc.setPapSheetId(inwuld.getKdysSheetId());
		// ut号
		tc.setUtNo(yaohl.getUth());		
		// 目的地
		tc.setMudd(yaohl.getCangkbh());		
		// 订货车间
		tc.setDinghcj(yaohl.getDinghcj());		
		// 预计到达时间
		tc.setYujddsj(yaohl.getZuiwsj());	
		// 路径代码
		tc.setLujdm(yaohl.getLujdm());	
		// 状态
		tc.setTczt("1");
		//新增 箱型
		tc.setXiangx(inwuld.getXiangx());
		if (wuld.getShunxh() != null){
			//新增  物理点顺序号 在200 的时候  更新  只更新一次  获取起运点和关箱时间
			if (wuld.getShunxh().compareTo("100") >= 0 && wuld.getShunxh().compareTo("199") < 0){
				tc.setGuanxsj(inwuld.gettTime());
				tc.setQiyd(inwuld.getcPointId());
			}
		}
		//最新物理点 跟同一个集装箱的 最新物理点
		//tc.setZuiswld(zuixwld);
		return tc;
	}
	
	
	/*
	 * 获取TC数据
	 * (TC号，KDYS提单号，最新物理点，启运点，启运时间，到达物理点时间，最新预计到达时间)
	 */
	private TC getTCForWuld(Inwuld inwuld, Wuld wuld, Waibwlxx waibwlxx){
		TC tc = new TC();
		// TC号
		tc.setTcNo(inwuld.getKdysBoxId());
		// kyds提单号
		tc.setKdysSheetId(inwuld.getKdysSheetId());
		// 最新物理点
		tc.setZuiswld(inwuld.getcPointId());
		//获取起运点和起运时间  新增 物理点顺序号 在200 的时候  更新   以前是 100-199
		if (wuld.getShunxh() != null){
//			if (wuld.getShunxh().compareTo("100") >= 0 && wuld.getShunxh().compareTo("199") < 0){
//				tc.setQiysj(inwuld.gettTime());
//				tc.setQiyd(inwuld.getcPointId());
//			}
			//新增  物理点顺序号 在200 的时候  更新  只更新一次  获取起运点和关箱时间
			if (wuld.getShunxh().compareTo("100") >= 0 && wuld.getShunxh().compareTo("199") < 0){
				tc.setGuanxsj(inwuld.gettTime());
				tc.setQiyd(inwuld.getcPointId());
			}
			//新增  物理点顺序号 在200 的时候  更新  只更新一次  获取起运点和起运时间
			if (wuld.getShunxh().compareTo("200") == 0){
				tc.setQiysj(inwuld.gettTime());
				tc.setQiyd(inwuld.getcPointId());
			}
		}
		// 到达物理点时间
		tc.setDaodwldsj(inwuld.gettTime());
		// 最新预计到达时间     tc的到达物理点时间   加上   物流信息中的计划到货剩余时间
		int Jihdhsysj = 0 ;
		if (waibwlxx.getJihdhsysj() != null){
			Jihdhsysj = (int)Math.ceil(Double.parseDouble(waibwlxx.getJihdhsysj()));
		}
		tc.setZuixyjddsj(DateUtil.dateAddDays(tc.getDaodwldsj(),Jihdhsysj));					
		return tc;
	}
	
	/*
	 * 获取要货令
	 * 实际发运时间，由KDYS更新（SHIJFYSJ）
	 * 修改货源交付时间，由KDYS更新（XIUGHYJJFSJ）
	 * 交付状态，由KDYS更新，0：延迟，1：提前，2：正常（JIAOFZT）
	 */
	private List<Yaohl> getYaohl(List<Yaohl> yaohlList, TC tc){
		/**
		 * 1、计算当前TC实际发运时间，货源交付时间，交付状态
		 * 2、更新要货令表   (只更新状态是在途的：02)
		 */
		List<Yaohl> list = new ArrayList<Yaohl>();
		for(Yaohl yaohl : yaohlList){
			if (yaohl.getYaohlh() != null && !"".endsWith(yaohl.getYaohlh())){
				if ("02".endsWith(yaohl.getYaohlzt())){
					yaohl.setXiughyjjfsj(tc.getZuixyjddsj());		
					yaohl.setShijfysj(tc.getQiysj());			//实际发运时间，由KDYS更新（启运时间）
						if (tc.getZuixyjddsj().compareTo(yaohl.getZuiwsj()) > 0) {
							yaohl.setJiaofzt("0");					//由KDYS更新，0：延迟，1：提前，2：正常
						} else if (tc.getZuixyjddsj().compareTo(yaohl.getZuiwsj()) < 0) {
							yaohl.setJiaofzt("1");					//由KDYS更新，0：延迟，1：提前，2：正常
						} else {
							yaohl.setJiaofzt("2");					//由KDYS更新，0：延迟，1：提前，2：正常
						}
						list.add(yaohl);
				}
			}
		}
		return list;
	}
	
	/*
	 * 获取集装箱管理数据
	 */
	private Jizxgl getJizxgl(TC tc ,Yaohl yaohl){
		Jizxgl jizxgl = new Jizxgl();
		//集装箱号
		jizxgl.setJizxh(tc.getTcNo());
		//用户中心
		jizxgl.setUsercenter(yaohl.getUsercenter());
		//状态
		jizxgl.setZhuangt("0");
		//是否纠纷中
		jizxgl.setShifjfz("0");
		//查询判断是否存在纠纷(存在非1、重箱或2、空箱的数据)
		Jizxgl jiufJzxgl = new Jizxgl();
		jizxgl.setJiufjzxh(tc.getTcNo());
		jizxgl.setUsercenter(yaohl.getUsercenter());
		jiufJzxgl = (Jizxgl)baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).selectObject("kdysService.queryJizxgl", jizxgl);
		if (jiufJzxgl != null && !"3".equals(jiufJzxgl.getZhuangt())){
			jizxgl.setShifjfz("1");
		}
		return jizxgl;
	}
	
	/**
	 * 获取ID
	 * 
	 * @param createTime
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private String getId(String createTimeNoLine) {
		// 获取yyyyMMdd时间
		Map<String, String> map = new HashMap<String, String>();
		map.put("createTimeNoLine", createTimeNoLine);
		// 查询当天最大的TC号
		Map<String, String> tempIdMap = (Map) baseDao.getSdcDataSource(ConstantDbCode.DATASOURCE_CK).selectObject("kdysService.getTcId", map);
		String tempId = tempIdMap.get("ID");
		String id = "";
		if (tempId == null || "".equals(tempId)) {
			id = "0000";
		} else {
			// 获取其长度
			int length = tempId.length();
			// 获取其后4位
			String temp = tempId.substring(length - 4, length);
			// 去掉4位中的前面的零
			String newString = "";
			if (temp.equals("0000")) {
				newString = "0";
			} else {
				newString = temp.replaceFirst("^0*", "");;
			}
			// 变成整型+1;
			Integer idI = Integer.valueOf(newString) + 1;
			// 然后将其变成四位字符串，不够四位补零
			id = String.format("%04d", idI);
		}
		return id;
	}
}
