package com.athena.component.input;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.utils.LoaderProperties;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;

/**
 * 整车单量份(输入) 3330
 * 
 * @date 2015-12-4
 * @author lc
 */
public class ZhengcdlfDbDataReader extends TxtInputDBSerivce {
	public Date date = new Date();
	private String datasourceId = "";
	private String logInfo = "";
	private String logDate = "";

	public ZhengcdlfDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}

	/**
	 * 解析数据之前清空IN_NUP_TMP表数据
	 */
	@Override
	public void before() {
		try {
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.execute("inPutddbh.inNupTmpDelete");
		} catch (RuntimeException e) {
			logger.error("线程--接口" + interfaceId + "清除IN_NUP_TMP表时报错"
					+ e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId + "清除IN_NUP_TMP表时报错"
					+ e.getMessage());
		}
	}

	/**
	 * 行记录解析之后 给行记录增加创建人、创建时间、修改人、修改时间 展开日期八位改为十位
	 */
	@Override
	public boolean afterRecord(Record record) {
		String zhankrq = record.getString("zhankrq").trim();
		try {
			record.put("zhankrq", DateUtil.StringFormatWithLine(zhankrq));
		} catch (ParseException e) {
			logger.error("接口"+interfaceId+"展开日期转换出错"+e.getMessage());
		}
		String lcdvbzk = record.getString("lcdvbzk").trim();
		if ("".equals(lcdvbzk)) {
			record.put("lcdvbzk", "BZK");
		}
		record.put("creator", interfaceId);
		record.put("create_time", date);
		record.put("editor", interfaceId);
		record.put("edit_time", date);
		return true;
	}

	/**
	 * 接口完成后处理方法 查询得到单位换算后的单位和数量
	 */
	@SuppressWarnings("unchecked")
	public void after() {
		//将IN_NP_TMP中半展开码为BZK的数据的半展开码改为空
		try {
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.updateINNUPTMPBZK");
		} catch (RuntimeException e) {
			logger.error("线程--接口" + interfaceId + "更改IN_NUP_TMP表中半展开码时报错" + e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId + "更改IN_NUP_TMP表中半展开码时报错" + e.getMessage());
		}
		// 查询得到单位换算后的单位和数量
		List<Map<String, Object>> lingjdwList = dataParserConfig.getBaseDao()
				.getSdcDataSource(datasourceId).select("inPutddbh.queryshuldanw");
		// (更新零件单位和数量
		for (Map<String, Object> map : lingjdwList) {
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.execute("inPutddbh.updateshuldanw", map);
		}

		String fileName="config/exchange/urlPath.properties";
		String danglf_builder=LoaderProperties.getPropertiesMap(fileName).get("danglf_builder");
		
		if (danglf_builder.equals("DFPV")) {
			
			/********************************************** ↓程思远于2015-12-21修改 ************************************************/
			String SID = TxtWriterDBTask.getUUID();// 唯一标示
			String EID = TxtWriterDBTask.getUUID();// 唯一标示
			int gcbgh = 0, liush = 0, maxNumber = 0, maxHang = 0;
			boolean flag = true, flag2 = true;
			String xlhNew = "", maxNum = "", maxXLH = "", maxXULH = "", xlhOld = "";
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			
			// 清空单量份差异表
			try {
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.ckxdanlfcyDelete");
			} catch (RuntimeException e) {
				logger.error("线程--接口" + interfaceId + "清除ckx_danlfcy表时报错" + e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId + "清除ckx_danlfcy表时报错" + e.getMessage());
			}
			// 首先从单量份接口表中查出当天传入的组合主键
			List<Map<String, Object>> danlfjkzjList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.QUERYdanlfjkbYLBZ");
			// 循环组合主键
			for (int i = 0; i < danlfjkzjList.size(); i++) {
				// 根据组合主键从单量份接口表中查出相应数据
				Map<String, Object> params = danlfjkzjList.get(i);
				List<Map<String, Object>> listDLF_LS = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.QUERYdanlfjkbAll", params);
				// 每次循环时查询当前最大序列号的序号
				List<Map<String, Object>> maxList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.QUERYgetMaxXULH");
				// 根据组合主键从单量份正式表中查询当前最大序列号
				List<Map<String, Object>> maxListByCondition = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.QUERYgetMaxXULHByCondition", params);
				// 根据lcdv16查询五位车型码
				Map<String, Object> paramsLCDV16 = new HashMap<String, Object>();
				paramsLCDV16.put("LCDV16", danlfjkzjList.get(i).get("LCDV24").toString().substring(0, 16));
				List<Map<String, Object>> fiveList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.QUERYxuanzcxByLCDV16", paramsLCDV16);
				String five = "";
				//如果不存在匹配的五位车型码，则跳过本次车型版本，继续下次车型版本的相关比较
				try {
					five = fiveList.get(0).get("ZHONGZCX").toString();
				} catch (Exception e) {
					logger.error("第" + (i+1) + "次执行：【LCDV24】"+danlfjkzjList.get(i).get("LCDV24").toString()+"没有匹配的五位车型码，请检查车型定义表中是否存在相应数据，跳过本次比对，执行下次比对" + e.getMessage());
					logInfo = "第" + (i+1) + "次执行：【LCDV24】"+danlfjkzjList.get(i).get("LCDV24").toString()+"没有匹配的五位车型码，请检查车型定义表中是否存在相应数据，跳过本次比对，执行下次比对";
					File_ErrorInfo(EID, "3330", SID, logInfo, logDate);
					continue;
				}
				// 查询出当前整车LCDV序列号表中的最大流水号，并加1
				List<Map<String, Object>> liushList = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.queryGetMaxLiush");
				liush = Integer.parseInt(liushList.get(0).get("MAXLIUSH").toString()) + 1;
				/******************************* 存在符合条件的最大序列号 ********************************/
				if (maxListByCondition.size() > 0) {// 存在符合条件的最大序列号
					// 查询出所有符合条件序列号
					List<Map<String, Object>> xulhAll = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.QUERYgetAllXULH", params);
					// 循环序列号集合
					flag = true;
					flag2 = true;
					xlhNew = "";
					for (int j = 0; j < xulhAll.size(); j++) {
						// 根据符合条件序列号查询单量份正式表中所有数据
						Map<String, Object> paramsXULH = new HashMap<String, Object>();
						paramsXULH.put("XULH", xulhAll.get(j).get("XULH").toString());
						List<Map<String, Object>> listDLF = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.QUERYdanlfCLSH", paramsXULH);
						// 比较两个集合中的车间号，零件编号，零件数量
						List<Map<String, Object>> listDLFJKtest = new ArrayList<Map<String, Object>>();
						List<Map<String, Object>> listDLFtest = new ArrayList<Map<String, Object>>();
						for (int k = 0; k < listDLF_LS.size(); k++) {
							Map<String, Object> mapJKtest = new HashMap<String, Object>();
							mapJKtest.put("CHEJ", listDLF_LS.get(k).get("CHEJ"));
							mapJKtest.put("LINGJBH",listDLF_LS.get(k).get("LINGJBH"));
							mapJKtest.put("SHUL", listDLF_LS.get(k).get("SHUL"));
							listDLFJKtest.add(mapJKtest);
						}
						for (int k = 0; k < listDLF.size(); k++) {
							Map<String, Object> maptest = new HashMap<String, Object>();
							maptest.put("CHEJ", listDLF.get(k).get("CHEJ"));
							maptest.put("LINGJBH", listDLF.get(k).get("LINGJBH"));
							maptest.put("SHUL", listDLF.get(k).get("SHUL"));
							listDLFtest.add(maptest);
						}
						/******************************* listDLFJKtest和listDLFtest中数据完全相同 ********************************/
						if (listDLFJKtest.containsAll(listDLFtest)
								&& listDLFtest.containsAll(listDLFJKtest)) {// listDLFJKtest和listDLFtest中数据完全相同
							//查询当前序列号是否是序列号表中流水号最大的序列号
							List<Map<String, Object>> lastListByCondition = dataParserConfig.getBaseDao()
							.getSdcDataSource(datasourceId).select("inPutddbh.QUERYgetLastXULHByCondition", params);
							// 使当前单量份数据的该序列号
							Map<String, Object> danlfParams = new HashMap<String, Object>();
							danlfParams = listDLF_LS.get(0);
							danlfParams.put("XULH", listDLF.get(0).get("XULH"));
							danlfParams.put("CREATE_TIME", new Date());
							danlfParams.put("LIUSH", liush);
							dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
									.execute("inPutddbh.insertIntoCkxlcdvxh",danlfParams);
							flag = false;
							if (!listDLF.get(0).get("XULH").equals(lastListByCondition.get(0).get("XULH"))) {
								xlhNew = listDLF.get(0).get("XULH").toString();
								flag2 = false;
							}
							break;
						}
					}
					/******************************* listDLFJKtest和listDLFtest中数据不完全相同 ********************************/
					Map<String, Object> danlfParams = new HashMap<String, Object>();
					danlfParams = listDLF_LS.get(0);
					
					if (flag) {
						flag2 = false;
						// 获取当前的最大序号，并加1
						maxNum = maxList.get(0).get("MAXXULH").toString();
						maxNumber = Integer.parseInt(maxNum) + 1;
						maxXLH = "" + maxNumber;
							
						while (maxXLH.length()<10) {
							maxXLH = '0' + maxXLH;
						}
						// 获得要使用的序列号
						maxXULH = five + maxXLH;
							
						xlhNew = maxXULH;
							
						/***************************************************************************/
						//此循环中，每次循环都要生成一个新的工程变更号（BOM创建）
						List<Map<String, Object>> maxGCBGH = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.getMaxGongcbgh");
						//获得该工程变更号后，将该工程变更号+1
						gcbgh = 1 + Integer.parseInt(maxGCBGH.get(0).get("MAXGCBGH").toString());
						//将该工程变更号加入工程变更号表中,状态记为“C”
			        	this.insertIntoGongcbgh(gcbgh, " ", 1, sdf.format(new Date()), maxXULH, "C", new Date());
			        	/***************************************************************************/
							
						// 将该单量份接口表中数据加入单量份正式表中
						danlfParams.put("XULH", maxXULH);
						danlfParams.put("GONGCBGH", gcbgh);
						dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.insertIntoCkxdanlf",danlfParams);
							
						// 将数据加入lcdv序列号表中
						danlfParams.put("CREATE_TIME", new Date());
						danlfParams.put("LIUSH", liush);
						dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.insertIntoCkxlcdvxh",danlfParams);
							
					}
					
					if (!flag2) {
						// 比较序列号表中最新数据和所有之前数据的差异，将差异加入单量份差异表中
						// 首先查出序列号表中所有符合条件数据
						List<Map<String, Object>> listMax = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.QUERYgetMaxLcdvs",danlfParams);
						// 获得新的序列号，该序列号对应的是本次加入的数据
						// 根据序列号从单量份正式表中查询相应数据
						Map<String, Object> xlhMap = new HashMap<String, Object>();
						// 查询最新序列号对应数据
						xlhMap.put("XULH", xlhNew);
						List<Map<String, Object>> listNew = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.QUERYdanlfCLSH", xlhMap);
						xlhOld = "";
						//循环总次数减少1
						for (int j = 0; j < listMax.size(); j++) {
							
							for (int k = 0; k < listNew.size(); k++) {
								listNew.get(k).remove("FLAG");
							}
							// 查询其他序列号对应数据
							xlhOld = listMax.get(j).get("XULH").toString();
							
							//当本次序列号与之前序列号不相同，说明本次数据与上次数据不同，才进行比较，否则不比较
							if (!xlhNew.equals(xlhOld)) {
								xlhMap.put("XULH", xlhOld);
								List<Map<String, Object>> listOld = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.QUERYdanlfCLSH", xlhMap);
								for (int k = 0; k < listOld.size(); k++) {
									listOld.get(k).remove("FLAG");
								}
								// 给车间、零件编号、数量都相同的行添加标记
								for (int k = 0; k < listNew.size(); k++) {
									for (int k2 = 0; k2 < listOld.size(); k2++) {
										if (listNew.get(k).get("FLAG") == null
												&& listOld.get(k2).get("FLAG") == null
												&& listNew.get(k).get("CHEJ").equals(listOld.get(k2).get("CHEJ"))
												&& listNew.get(k).get("LINGJBH").equals(listOld.get(k2).get("LINGJBH"))
												&& listNew.get(k).get("SHUL").equals(listOld.get(k2).get("SHUL"))) {
											listNew.get(k).put("FLAG", "flag");
											listOld.get(k2).put("FLAG", "flag");
											break;
										}
									}
								}
								/***************************************************************************/
								//此循环中，每次循环都要生成一个新的工程变更号（BOM更改）
								//首先查询当前最大工程变更号
								List<Map<String, Object>> maxGCBGH2 = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.getMaxGongcbgh");
								//获得该工程变更号后，将该工程变更号+1
								gcbgh = 1 + Integer.parseInt(maxGCBGH2.get(0).get("MAXGCBGH").toString());
								//将该工程变更号加入工程变更号表中,状态记为“E”
				        		this.insertIntoGongcbgh(gcbgh, " ", 1, sdf.format(new Date()), xlhNew, "E", new Date());
				        		/***************************************************************************/

								// 向差异表中添加记录
								// 获得当前最大行号
								maxHang = 0;
								try {
									maxHang = Integer.parseInt(listOld.get(0).get("HANGH").toString());
								} catch (Exception e) {
									logger.error("线程--接口" + interfaceId + "序列号无对应数据" + e.getMessage());
									continue;
								}
								for (int k = 0; k < listOld.size(); k++) {
									if (maxHang < Integer.parseInt(listOld.get(k).get("HANGH").toString())) {
										maxHang = Integer.parseInt(listOld.get(k).get("HANGH").toString());
									}
								}
								
								maxHang += 1;
								// 记录为新增，此时需要记为新增行号
								for (int k = 0; k < listNew.size(); k++) {
									if (listNew.get(k).get("FLAG") == null) {
										Map<String, Object> cyMap = new HashMap<String, Object>();
										cyMap = listNew.get(k);
										cyMap.put("BENCXH",listNew.get(k).get("XULH"));
										cyMap.put("SHANGCXH",listOld.get(listOld.size()-1).get("XULH"));
										cyMap.put("HANGH", maxHang);
										cyMap.put("CAOZM", "A");
										cyMap.put("CREATE_TIME", new Date());
										cyMap.put("GONGCBGH", gcbgh);
										dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
												.execute("inPutddbh.insertIntoDLFCY",cyMap);
										maxHang++;
									}
								}
								// 记录为删除，此时需要记为删除行号
								for (int k = 0; k < listOld.size(); k++) {
									if (listOld.get(k).get("FLAG") == null) {
										Map<String, Object> cyMap = new HashMap<String, Object>();
										cyMap = listOld.get(k);
										cyMap.put("BENCXH",listNew.get(listNew.size()-1).get("XULH"));
										cyMap.put("SHANGCXH",listOld.get(k).get("XULH"));
										cyMap.put("HANGH",listOld.get(k).get("HANGH"));
										cyMap.put("CAOZM", "D");
										cyMap.put("CREATE_TIME", new Date());
										cyMap.put("GONGCBGH", gcbgh);
										dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
												.execute("inPutddbh.insertIntoDLFCY",cyMap);
									}
								}
							}
						}
					}
					
					/******************************* 不存在符合条件的最大序列号，此时正式表中无相关数据 ********************************/
				} else {
					// 使用五位车型码
					maxXULH = "";
					maxXLH = "";
					if (maxList.size() > 0) {// 存在最大序列号
						// 使用最大序列号+1
						// 获取当前的最大序号，并加1
						maxNum = maxList.get(0).get("MAXXULH").toString();
						maxNumber = Integer.parseInt(maxNum) + 1;
						maxXLH = "" + maxNumber;
						while (maxXLH.length()<10) {
							maxXLH = '0' + maxXLH;
						}
					} else {// 不存在最大序列号
						// 新序列号0000000001
						maxXLH = "0000000001";
					}
					// 获得要使用的序列号
					maxXULH = five + maxXLH;
						
					/***************************************************************************/
					//此循环中，每次循环都要生成一个新的工程变更号（BOM创建）
					List<Map<String, Object>> maxGCBGH = dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).select("inPutddbh.getMaxGongcbgh");
					//获得该工程变更号后，将该工程变更号+1
					gcbgh = 1 + Integer.parseInt(maxGCBGH.get(0).get("MAXGCBGH").toString());
					//将该工程变更号加入工程变更号表中,状态记为“C”
					this.insertIntoGongcbgh(gcbgh, " ", 1, sdf.format(new Date()), maxXULH, "C", new Date());
		        	/***************************************************************************/
						
					// 将该单量份接口表中数据加入单量份正式表中
					Map<String, Object> danlfParams = new HashMap<String, Object>();
					danlfParams = listDLF_LS.get(0);
					danlfParams.put("XULH", maxXULH);
					danlfParams.put("GONGCBGH", gcbgh);
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.insertIntoCkxdanlf", danlfParams);
						
					// 将数据加入lcdv序列号表中
					danlfParams.put("CREATE_TIME", new Date());
					danlfParams.put("LIUSH", liush);
					dataParserConfig.getBaseDao().getSdcDataSource(datasourceId).execute("inPutddbh.insertIntoCkxlcdvxh",danlfParams);
				}
			}
			
			try {
				//删除IN_NUP表中USERCENTER、LCDV24、LCDVBZK、ZHANKRQ与IN_NUP_TMP表中相同的数据
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutddbh.deleteInNupIINT");
				//将IN_NUP_TMP表中所有数据插入IN_NUP表中
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutddbh.insertInNupIINT");
				//清除IN_NUP表中创建时间为45天之前的数据
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutddbh.deleteInNupOld");
			} catch (Exception e) {
				logger.error("线程--接口" + interfaceId + "IN_NUP_TMP表向IN_NUP表插入数据相关操作出现错误"
						+ e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId
						+ "IN_NUP_TMP表向IN_NUP表插入数据相关操作出现错误" + e.getMessage());
			}
			
			// 将超过30天未使用的表中数据清除
			try {
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
						.execute("inPutddbh.danlfDeleteXLH");

				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
						.execute("inPutddbh.lcdvxhDeleteXLH");
			} catch (Exception e) {
				logger.error("线程--接口" + interfaceId + "清除30天未使用的表中数据出现错误"
						+ e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId
						+ "清除30天未使用的表中数据出现错误" + e.getMessage());
			}

			/********************************************** ↑程思远于2015-12-21修改 ************************************************/

		}else {
			try {
				logger.info("开始清除in_nup表与IN_NUP_TMP表相同数据，将最新数据写到in_nup...");
				//数据更新入in_nup
				//删除IN_NUP表中USERCENTER、LCDV24、LCDVBZK、ZHANKRQ与IN_NUP_TMP表中相同的数据
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutddbh.deleteInNupIINT");
				//将IN_NUP_TMP表中所有数据插入IN_NUP表中
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutddbh.insertInNupIINT");
				//清除IN_NUP表中创建时间为100天之前的数据
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutddbh.deleteInNupOldHund");
			} catch (Exception e) {
					logger.error("线程--接口" + interfaceId + "IN_NUP_TMP表向IN_NUP表插入数据相关操作出现错误"
							+ e.getMessage());
					throw new ServiceException("线程--接口" + interfaceId
							+ "IN_NUP_TMP表向IN_NUP表插入数据相关操作出现错误" + e.getMessage());
			}
			logger.info("in_nup数据更新结束...");
			
		}
		
	}

	/**
	 * 空串处理
	 * 
	 * @param obj
	 *            对象
	 * @return 处理后字符串
	 * @date 2011-10-26
	 */
	private String strNull(Object obj) {// 对象为空返回空串,不为空toString
		return obj == null ? "" : obj.toString().trim();
	}

	/**
	 * 记录数据日志表
	 */
	public void File_ErrorInfo(String EID, String CID, String SID,
			String file_errorinfo, String error_date) {
		Map<String, String> params = new HashMap<String, String>();
		params.put("EID", strNull(EID));
		params.put("INBH", strNull(CID));
		params.put("SID", strNull(SID));
		params.put("file_errorinfo", strNull(file_errorinfo));
		params.put("error_date", strNull(error_date));
		try {
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.execute("inPutddbh.insertErrorFileInfo", params);
		} catch (RuntimeException e) {
			logger.error("线程--接口" + dataParserConfig.getId()
					+ "写in_errorfile表报错" + e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId()
					+ "写in_errorfile表报错" + e.getMessage());
		}
	}
	
	/**
	 * 将该工程变更号加入工程变更号表中
	 * @param aennr
	 * @param aetxt
	 * @param aenst
	 * @param datuv
	 * @param xulh
	 * @param zhuangt
	 * @param yunxsj
	 */
	private void insertIntoGongcbgh(int aennr, String aetxt, int aenst, String datuv, 
			String xulh, String zhuangt, Date yunxsj){
		Map<String,Object> parameter = new HashMap<String, Object>();
		parameter.put("AENNR", aennr);
		parameter.put("AETXT", aetxt);
		parameter.put("AENST", aenst);
		parameter.put("DATUV", datuv);
		parameter.put("XULH", xulh);
		parameter.put("ZHUANGT", zhuangt);
		parameter.put("YUNXSJ", yunxsj);
		dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
			.execute("inPutddbh.insertIntoGongcbgh", parameter);
		parameter.clear();
	}

}
