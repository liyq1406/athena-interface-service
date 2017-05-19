package com.athena.component.input;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtInputDBSerivce;
import com.athena.component.utils.LoaderProperties;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;

/**
 * 3320 总成单量份
 * 
 * @date 2015-12-5
 * @author yz
 */
public class ZongcdlfDbDataReader extends TxtInputDBSerivce {
	public Date date = new Date();
	protected static Logger logger = Logger
			.getLogger(ZongcdlfDbDataReader.class); // 定义日志方法
	private String datasourceId = "";

	public ZongcdlfDbDataReader(DataParserConfig dataParserConfig) {
		super(dataParserConfig);
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}

	/**
	 * 解析数据之前清空in_nup_zongcdlf表数据
	 */
	@Override
	public void before() {
		try {
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.execute("inPutddbh.inNupZongcdlfDelete");
		} catch (RuntimeException e) {
			logger.error("线程--接口" + interfaceId + "清除in_nup_zongcdlf表时报错"
					+ e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId
					+ "清除in_nup_zongcdlf表时报错" + e.getMessage());
		}
	}

	/**
	 * 行解析之前处理方法 子件使用车间为空就不读取
	 * mantis:0012907 如果ecom的年份大于等于2050也不读取 by CSY 20160929
	 */
	@Override
	public boolean beforeRecord(String line, String fileName, int lineNum) {
		boolean result = true;
		String zijsycj = line.toString().substring(21, 24).trim();
		if (line != null && zijsycj.equals("")) {
			result = false;
		}
		String ecomYear = line.toString().substring(13, 17).trim();
		try {
			if (Integer.parseInt(ecomYear) >= 2050) {
				//ecom的年转换成数字，如果转换成功，且数字大于等于2050，则不读取
				result = false;
			}
		} catch (Exception e) {
			//ecom的年转换成数字，如果转换失败，说明该行文本有误
			logger.error("接口"+interfaceId+"ecom的转换出错"+e.getMessage());
			result = false;
		}
		return result;
	}

	/**
	 * 行解析之后处理方法 给行记录增加创建人、创建时间、整车装配中心 ECOM文本8位数据库10位
	 */
	@Override
	public boolean afterRecord(Record record) {
		String usercenter = record.getString("zijsycj").trim().substring(0, 2);
		String ecom = record.getString("ecom").trim();
		try {
			record.put("ecom", DateUtil.StringFormatWithLine(ecom));
		} catch (ParseException e) {
			logger.error("接口"+interfaceId+"展开日期转换出错"+e.getMessage());
		}
		record.put("usercenter", usercenter);
		record.put("create_time", date);
		record.put("creator", interfaceId);
		return true;
	}

	/**
	 * 接口运行后 更新数量还有单位
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void after() {
		// 查询出数量还有单位
		List<Map<String, Object>> lingjdwList = dataParserConfig.getBaseDao()
				.getSdcDataSource(datasourceId).select("inPutddbh.queryDanwShulzongcdlf");
		for (Map<String, Object> map : lingjdwList) {
			// 更新in_nup_zongcdlf表
			dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
					.execute("inPutddbh.updatezongcdlf", map);
		}

		String fileName="config/exchange/urlPath.properties";
		String danglf_builder=LoaderProperties.getPropertiesMap(fileName).get("danglf_builder");
		
		if (danglf_builder.equals("DFPV")) {
			
			/********************************************** ↓程思远于2016-01-04修改 ************************************************/
			Integer maxHang = 0;
			int gcbgh = 0;
			boolean flag = true;
			
			// 清空总成单量份差异表
			try {
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
						.execute("inPutddbh.ckxZongcdlfcDelete");
			} catch (RuntimeException e) {
				logger.error("线程--接口" + interfaceId + "清除in_ckx_zongcdlf表时报错"
						+ e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId
						+ "清除in_ckx_zongcdlf表时报错" + e.getMessage());
			}
			
			// 首先从取出总成单量份接口表中所有总成号（按总成号排序）
			List<Map<String, Object>> inZongchList = dataParserConfig.getBaseDao()
					.getSdcDataSource(datasourceId).select("inPutddbh.QueryZongcdlfZCLJH");
			//循环总成号列表
			for (int i = 0; i < inZongchList.size(); i++) {
				gcbgh = 0;
				//使用总成号列表中的总成号从总成单量份正式表中查询半展开日期最大数据集合
				List<Map<String, Object>> ckxZongchMaxList = dataParserConfig.getBaseDao()
				.getSdcDataSource(datasourceId).select("inPutddbh.QueryZongcdlfMax",inZongchList.get(i));
				
				//从中间表中仅取出总成号相同，半展开日期大于正式表中相应最大日期的数据的排序后的组合（总成号、展开日期）
				//获得组合排序后的集合
				List<Map<String, Object>> inZongcdlfZB = dataParserConfig.getBaseDao()
				.getSdcDataSource(datasourceId).select("inPutddbh.QueryZongcdlfZB",inZongchList.get(i));
				
				//将这些组合对应的单量份接口表中的数据插入单量份正式表中（需要依次生成行号rownum）
				//如果总成单量份正式表中无相应最大数据，则生成一个工程变更号（创建），并将该工程变更号插入正式表中
				for (int j = 0; j < inZongcdlfZB.size(); j++) {
					
					//如果正式表中不存在最大数据，且是当前第一次循环
					if (ckxZongchMaxList.size() == 0 && j == 0) {
						//此时要生成一个新的工程变更号，状态记为创建（C）
						/***************************************************************************/
						//生成一个新的工程变更号（BOM创建）
						List<Map<String, Object>> maxGCBGH = dataParserConfig.getBaseDao()
						.getSdcDataSource(datasourceId).select("inPutddbh.getMaxGongcbgh");
						//获得该工程变更号后，将该工程变更号+1
						gcbgh = 1 + Integer.parseInt(maxGCBGH.get(0).get("MAXGCBGH").toString());
						//该工程变更号记录为'C'
						//将该工程变更号加入工程变更号表中
		        		this.insertIntoGongcbgh(gcbgh, " ", 1, inZongcdlfZB.get(j).get("ECOM").toString().replaceAll("-", ""), inZongcdlfZB.get(j).get("ZONGCLJ").toString(), "C", new Date());
		        		/***************************************************************************/
					}else{
						gcbgh = 0;
					}
					//将相应数据插入正式表中
					Map<String, Object> paramsZB = inZongcdlfZB.get(j);
					paramsZB.put("GONGCBGH", "");
					if (gcbgh != 0) {
						//将要生成的工程变更号不为0，说明已经生成了新的工程变更号
						//此时需要向正式表中插入新的工程变更号
						paramsZB.put("GONGCBGH", gcbgh);
					}
					try {
						dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
								.execute("inPutddbh.insertIntoZongcdlf",paramsZB);
					} catch (RuntimeException e) {
						logger.error("线程--接口" + interfaceId + "向ckx_zongcdlf表插入数据时报错"
								+ e.getMessage());
						throw new ServiceException("线程--接口" + interfaceId
								+ "向ckx_zongcdlf表插入数据时报错" + e.getMessage());
					}
				}
				
				//下面的数据基本都是从总成单量份正式表中查出
				//循环ckxZongchMaxList，将本次总成组合集合中的第一条数据对应数据与之进行比较，差异加入差异表中（每次生成一个新的工程变更号）
				if (ckxZongchMaxList.size()>0) {
					//根据第一行组合查询正式表中的数据
					if (inZongcdlfZB.size()>0) {
						List<Map<String, Object>> ckxZongchFirst = dataParserConfig.getBaseDao()
						.getSdcDataSource(datasourceId).select("inPutddbh.QueryZongcdlfByCondition",inZongcdlfZB.get(0));
						
						try {
							// 比较总成单量份表的对应总成单量份（上次）表中都只有一组数据，因此不需要循环比较
							// 直接取之前查询出的数据进行比较，将差异加入到总成单量份差异表中

							//清除之前的标记
							for (int k = 0; k < ckxZongchMaxList.size(); k++) {
								ckxZongchMaxList.get(k).remove("FLAG");
							}
							
							for (int k = 0; k < ckxZongchFirst.size(); k++) {
								ckxZongchFirst.get(k).remove("FLAG");
							}
							
							// 给车间、零件编号、数量都相同的行添加标记
							for (int k = 0; k < ckxZongchMaxList.size(); k++) {
								for (int k2 = 0; k2 < ckxZongchFirst.size(); k2++) {
									if (ckxZongchMaxList.get(k).get("FLAG") == null
											&& ckxZongchFirst.get(k2).get("FLAG") == null
											&& ckxZongchMaxList.get(k).get("ZIJSYCJ").equals(ckxZongchFirst.get(k2).get("ZIJSYCJ"))
											&& ckxZongchMaxList.get(k).get("LINGJBH").equals(ckxZongchFirst.get(k2).get("LINGJBH"))
											&& ckxZongchMaxList.get(k).get("SHUL").equals(ckxZongchFirst.get(k2).get("SHUL"))) {
										ckxZongchMaxList.get(k).put("FLAG", "flag");
										ckxZongchFirst.get(k2).put("FLAG", "flag");
										break;
									}
								}
							}
							//只要有一个标记存在，即为有差异
							flag = true;
							for (int k = 0; k < ckxZongchMaxList.size(); k++) {
								if (ckxZongchMaxList.get(k).get("FLAG")==null) {
									flag = false;
									break;
								}
							}
							for (int k = 0; k < ckxZongchFirst.size(); k++) {
								if (ckxZongchFirst.get(k).get("FLAG")==null) {
									flag = false;
									break;
								}
							} 
							//有差异的情况
							if (!flag) {
								/***************************************************************************/
								//生成一个新的工程变更号（BOM更改）
								//首先查询当前最大工程变更号
								List<Map<String, Object>> maxGCBGH2 = dataParserConfig.getBaseDao()
								.getSdcDataSource(datasourceId).select("inPutddbh.getMaxGongcbgh");
								//获得该工程变更号后，将该工程变更号+1
								gcbgh = 1 + Integer.parseInt(maxGCBGH2.get(0).get("MAXGCBGH").toString());
								//该工程变更号记录为'E'
								//将该工程变更号加入工程变更号表中
								this.insertIntoGongcbgh(gcbgh, " ", 1, ckxZongchFirst.get(0).get("ECOM").toString().replaceAll("-", ""), ckxZongchFirst.get(0).get("ZONGCLJ").toString(), "E", new Date());
				        		/***************************************************************************/

								// 向差异表中添加记录
								// 获得当前最大行号
								maxHang = Integer.parseInt(ckxZongchFirst.get(0).get("HANGH").toString());
								for (int k = 0; k < ckxZongchFirst.size(); k++) {
									if (maxHang < Integer.parseInt(ckxZongchFirst.get(k).get("HANGH").toString())) {
										maxHang = Integer.parseInt(ckxZongchFirst.get(k).get("HANGH").toString());
									}
								}
								maxHang += 1;
								// 记录为新增，此时需要记为新增行号
								for (int k = 0; k < ckxZongchFirst.size(); k++) {
									if (ckxZongchFirst.get(k).get("FLAG") == null) {
										Map<String, Object> cyMap = new HashMap<String, Object>();
										cyMap = ckxZongchFirst.get(k);
										cyMap.put("HANGH", maxHang);
										cyMap.put("CAOZM", "A");
										cyMap.put("CREATE_TIME", new Date());
										cyMap.put("GONGCBGH", gcbgh);
										dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
												.execute("inPutddbh.insertIntoZongcDLFC",cyMap);
										maxHang++;
									}
								}
								// 记录为删除，此时需要记为删除行号
								for (int k = 0; k < ckxZongchMaxList.size(); k++) {
									if (ckxZongchMaxList.get(k).get("FLAG") == null) {
										Map<String, Object> cyMap = new HashMap<String, Object>();
										cyMap = ckxZongchMaxList.get(k);
										cyMap.put("HANGH", ckxZongchMaxList.get(k).get("HANGH"));
										cyMap.put("CAOZM", "D");
										cyMap.put("CREATE_TIME", new Date());
										cyMap.put("GONGCBGH", gcbgh);
										dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
												.execute("inPutddbh.insertIntoZongcDLFC",cyMap);
									}
								}
							}
							
						} catch (Exception e) {
							logger.error("线程--接口" + interfaceId + "第" + i
									+ "次执行出现错误" + e.getMessage());
							throw new ServiceException("线程--接口" + interfaceId + "第"
									+ i + "次执行出现错误" + e.getMessage());
						}
					}
				}
				
				//循环上述组合集合（总次数-1）
				//每次循环将前一条组合对应的数据和后一条组合对应的数据进行比较，将差异记入差异表（每次生成一个新的工程变更号）
				for (int j = 0; j < inZongcdlfZB.size() - 1; j++) {
					
					//根据组合从正式表查询相邻两次的集合
					//上次
					List<Map<String, Object>> ckxZongchList1 = dataParserConfig.getBaseDao()
					.getSdcDataSource(datasourceId).select("inPutddbh.QueryZongcdlfByCondition",inZongcdlfZB.get(j));
					//本次
					List<Map<String, Object>> ckxZongchList2 = dataParserConfig.getBaseDao()
					.getSdcDataSource(datasourceId).select("inPutddbh.QueryZongcdlfByCondition",inZongcdlfZB.get(j+1));
					//比较差异
					try {
						// 比较总成单量份表的对应总成单量份（上次）表中都只有一组数据，因此不需要循环比较
						// 直接取之前查询出的数据进行比较，将差异加入到总成单量份差异表中

						for (int k = 0; k < ckxZongchList1.size(); k++) {
							ckxZongchList1.get(k).remove("FLAG");
						}
						
						for (int k = 0; k < ckxZongchList2.size(); k++) {
							ckxZongchList2.get(k).remove("FLAG");
						}
						
						// 给车间、零件编号、数量都相同的行添加标记
						for (int k = 0; k < ckxZongchList1.size(); k++) {
							for (int k2 = 0; k2 < ckxZongchList2.size(); k2++) {
								if (ckxZongchList1.get(k).get("FLAG") == null
										&& ckxZongchList2.get(k2).get("FLAG") == null
										&& ckxZongchList1.get(k).get("ZIJSYCJ").equals(ckxZongchList2.get(k2).get("ZIJSYCJ"))
										&& ckxZongchList1.get(k).get("LINGJBH").equals(ckxZongchList2.get(k2).get("LINGJBH"))
										&& ckxZongchList1.get(k).get("SHUL").equals(ckxZongchList2.get(k2).get("SHUL"))) {
									ckxZongchList1.get(k).put("FLAG", "flag");
									ckxZongchList2.get(k2).put("FLAG", "flag");
									break;
								}
							}
						}
						
						flag = true;
						for (int k = 0; k < ckxZongchList1.size(); k++) {
							if (ckxZongchList1.get(k).get("FLAG")==null) {
								flag = false;
								break;
							}
						}
						for (int k = 0; k < ckxZongchList2.size(); k++) {
							if (ckxZongchList2.get(k).get("FLAG")==null) {
								flag = false;
								break;
							}
						}
						if (!flag) {
							/***************************************************************************/
							//生成一个新的工程变更号（BOM更改）
							//首先查询当前最大工程变更号
							List<Map<String, Object>> maxGCBGH2 = dataParserConfig.getBaseDao()
							.getSdcDataSource(datasourceId).select("inPutddbh.getMaxGongcbgh");
							//获得该工程变更号后，将该工程变更号+1
							gcbgh = 1 + Integer.parseInt(maxGCBGH2.get(0).get("MAXGCBGH").toString());
							//该工程变更号记录为'E'
							//将该工程变更号加入工程变更号表中
			        		this.insertIntoGongcbgh(gcbgh, " ", 1, ckxZongchList2.get(0).get("ECOM").toString().replaceAll("-", ""), ckxZongchList2.get(0).get("ZONGCLJ").toString(), "E", new Date());
							// 向差异表中添加记录
							// 获得当前最大行号
							maxHang = Integer.parseInt(ckxZongchList2.get(0).get("HANGH").toString());
							for (int k = 0; k < ckxZongchList2.size(); k++) {
								if (maxHang < Integer.parseInt(ckxZongchList2.get(k).get("HANGH").toString())) {
									maxHang = Integer.parseInt(ckxZongchList2.get(k).get("HANGH").toString());
								}
							}
							maxHang += 1;
							// 记录为新增，此时需要记为新增行号
							for (int k = 0; k < ckxZongchList2.size(); k++) {
								if (ckxZongchList2.get(k).get("FLAG") == null) {
									Map<String, Object> cyMap = new HashMap<String, Object>();
									cyMap = ckxZongchList2.get(k);
									cyMap.put("HANGH", maxHang);
									cyMap.put("CAOZM", "A");
									cyMap.put("CREATE_TIME", new Date());
									cyMap.put("GONGCBGH", gcbgh);
									dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
											.execute("inPutddbh.insertIntoZongcDLFC",cyMap);
									maxHang++;
								}
							}
							// 记录为删除，此时需要记为删除行号
							for (int k = 0; k < ckxZongchList1.size(); k++) {
								if (ckxZongchList1.get(k).get("FLAG") == null) {
									Map<String, Object> cyMap = new HashMap<String, Object>();
									cyMap = ckxZongchList1.get(k);
									cyMap.put("HANGH", ckxZongchList1.get(k).get("HANGH"));
									cyMap.put("CAOZM", "D");
									cyMap.put("CREATE_TIME", new Date());
									cyMap.put("GONGCBGH", gcbgh);
									dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
											.execute("inPutddbh.insertIntoZongcDLFC",cyMap);
								}
							}
						}
					} catch (Exception e) {
						logger.error("线程--接口" + interfaceId + "第" + i
								+ "次执行出现错误" + e.getMessage());
						throw new ServiceException("线程--接口" + interfaceId + "第"
								+ i + "次执行出现错误" + e.getMessage());
					}
				}
			}
			
			//清除展开日期30天前的数据
			// 将超过30天未使用的表中数据清除
			try {
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
						.execute("inPutddbh.ZongCdlfDelete");
			} catch (Exception e) {
				logger.error("线程--接口" + interfaceId + "清除30天未使用的表中数据出现错误"
						+ e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId
						+ "清除30天未使用的表中数据出现错误" + e.getMessage());
			}
			/********************************************** ↑程思远于2016-01-04修改 ************************************************/

		}else{
			try {
				logger.info("开始更新ckx_zongcdlf数据...");
				//删除in_zongcdlf中ecom小于ckx_zongcdlf已存在最大ecom的数据
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutddbh.inZongCdlfDelete");
				//数据插入ckx_zongcdlf
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutddbh.insertInCKXZCDLF");
				//删除ckx_zongcdlf中30天前的数据
				dataParserConfig.getBaseDao().getSdcDataSource(datasourceId)
				.execute("inPutddbh.ZongCdlfDelete");
			} catch (Exception e) {
				logger.error("线程--接口" + interfaceId + "IN_NUP_ZONGCDLF表向ckx_zongcdlf表插入数据相关操作出现错误"
						+ e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId
						+ "IN_NUP_ZONGCDLF表向ckx_zongcdlf表插入数据相关操作出现错误" + e.getMessage());
			}
			logger.info("ckx_zongcdlf数据更新结束...");
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
