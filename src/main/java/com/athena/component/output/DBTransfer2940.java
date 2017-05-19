package com.athena.component.output;

import java.util.List;
import java.util.Map;

import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.DBOutputTxtSerivce;
import com.athena.util.exception.ServiceException;

/**
 * 此类中的方法可能与入库明细不相关，只是借助入库明细接口来进行其他处理
 * @author CSY
 * @date 2016-06-16
 *
 */
public class DBTransfer2940 extends DBOutputTxtSerivce {
	
	public DBTransfer2940(DataParserConfig dataParserConfig) {
	}
	
	/**
	 * 定义数据源
	 */
	private final static String datasourceZXC = "1";	//执行层数据源
	private final static String datasourceDDBH = "2";	//DDBH数据源

	@Override
	public void after() {
		yuplkjZxcToDdbh();	//从ZXC向DDBH传递预批量扣减数据（出现错误时整个2940都会停止）
	}
	
	/**
	 * 预批量零件扣减
	 */
	private void yuplkjZxcToDdbh(){
		logger.info("【2940】接口主要功能执行完毕，开始执行【预批量零件扣减传输】工作...");
		//首先将执行层CK_YUPLKJ中FLAG为0的数据的FLAG值改为1
		int num = 0;
		try {
			num = dataParserConfig.getBaseDao().getSdcDataSource(datasourceZXC).execute("outPut.updateYPLKJ0to1");
		} catch (RuntimeException e) {
			logger.error("线程--接口" + interfaceId + "将CK_YUPLKJ表flag由0更新为1时出错" + e.getMessage());
			throw new ServiceException("线程--接口" + interfaceId + "将CK_YUPLKJ表flag由0更新为1时出错" + e.getMessage());
		}
		logger.info("开始将ZXC中CK_YUPLKJ传递给DDBH_YUPLKJ，预计传输量为 " + num + " 条...");
		//再将执行层中FLAG为1的数据传入到DDBH层（传给DDBH的FLAG默认为0）
		String sqlA = "outPut.queryYPLKJfromZXC";	//从执行层查询的语句
		String sqlB = "outPut.insertDDBHYUPLKJ";	//插入DDBH层的语句
		num = fromAToB(datasourceZXC, sqlA, datasourceDDBH, sqlB);
		//最后将执行层中FLAG为1的数据的FLAG值改为2
		if (num > 0) {
			try {
				num = dataParserConfig.getBaseDao().getSdcDataSource(datasourceZXC).execute("outPut.updateYPLKJ1to2");
			} catch (RuntimeException e) {
				logger.error("线程--接口" + interfaceId + "将CK_YUPLKJ表flag由1更新为2时出错" + e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId + "将CK_YUPLKJ表flag由1更新为2时出错" + e.getMessage());
			}
		}
		logger.info("【预批量零件扣减传输】工作执行完毕，共传输 " + num + " 条数据");
	}
	  
	/**
	 * 从A层查询出数据后插入到B层
	 */
	private int fromAToB(String datasourceA, String sqlA, String datasourceB, String sqlB){
		//从A层查询出数据并存入集合
		List<Map<String, Object>> list = dataParserConfig.getBaseDao().getSdcDataSource(datasourceA).select(sqlA,interfaceId);
		int num = 0;
		if (null != list && list.size()>0) {
			try {
				num = dataParserConfig.getBaseDao().getSdcDataSource(datasourceB).execute(sqlB, list);
			} catch (RuntimeException e) {
				logger.error("线程--接口" + interfaceId + "两层间数据传输出错" + e.getMessage());
				throw new ServiceException("线程--接口" + interfaceId + "两层间数据传输出错" + e.getMessage());
			}
		}
		return num;
	}
	
}
