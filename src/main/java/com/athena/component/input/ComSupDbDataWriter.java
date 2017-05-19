package com.athena.component.input;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.athena.component.exchange.Record;
import com.athena.component.exchange.SpaceFinal;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.date.DateUtil;
import com.athena.util.exception.ServiceException;
import com.toft.core2.dao.database.DbUtils;
import com.toft.utils.UUIDHexGenerator;


/**
 * 零件供应商参考系输入类
 * 
 * @author WL
 * @date 2011-10-20
 */
public class ComSupDbDataWriter extends TxtWriterDBTask {

	private static int HUNDRED_PERCENT = 1;   //份额百分之百100%
	private static int ZERO_PERCENT = 0;      //份额为0
	protected static Logger logger = Logger.getLogger(ComSupDbDataWriter.class);	//定义日志方法
	public ComSupDbDataWriter(DataParserConfig dataParserConfig,
			List<String> fieldList, List<String> updateFieldList,
			String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
	}


	/**
	 * 行解析后处理方法
	 * 
	 * @param rowIndex
	 *            行标
	 * @param record
	 *            行数据
	 * @author GJ
	 */
	@Override
	public boolean beforeRecord(Record record) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		// 获取第一个字节为ZTYPE
		String type = record.getParam().get("TYPE").toString();
		// 获取用户中心
		String USERCENTER = record.getString("USERCENTER").trim();
		// 获取零件号
		String LINGJBH = record.getString("LINGJBH").trim();
		// 获取供应商编号
		String GONGYSBH = record.getString("GONGYSBH").trim();
		//获取供应份额
		String GONGYFE= record.getString("GONGYFE").trim();
		//获取生效时间
		String SHENGXSJ=record.getString("SHENGXSJ").trim(); 
		//定义生效日期格式转换 
		//获取供应份配，并且做数据格式转换
		double i_edu= 0.0;
		if(StringUtils.isNotEmpty(GONGYFE)){
			i_edu=Double.valueOf(GONGYFE)/100;
			//double t_edu=i_edu/100;
			record.put("GONGYFE", i_edu);
			record.put("SAP_FENE", i_edu);
		}else{
			record.put("GONGYFE", "0");
			record.put("SAP_FENE", "0");
			GONGYFE = "0";
		}
		try{  
			//生效时间格式转换
			if(StringUtils.isNotEmpty(SHENGXSJ)){ 
				record.put("SHENGXSJ", df.parse(DateUtil.StringFormatWithLine(SHENGXSJ)));
			}
		}catch(ParseException e ){
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据日期转换异常，生效时间为空  " + e.getMessage());
			throw new WarmBusinessException("日期转换错误,生效时间为空."+e.getMessage());
		}
		try{	
			// 如果ZTYPE = '1'时
			if ("1".equals(type)) {
					// 1、如果"用户中心"和"零件号"两个维度，并且当"合同号"不为空，而且”生效标识”为1(有效) 时 查到有数据时配额为0，标识为0(无效)
					Map<String,String> y_map =  this.queryLingjGYSFE(USERCENTER, LINGJBH);
					if (!y_map.isEmpty()) {
						// 根据"用户中心"、"零件号"、"供应商编码"三个维度去查询数据
						Map<String,String> t_map  = this.queryLingjGYS(USERCENTER, LINGJBH, GONGYSBH);
						if(!t_map.isEmpty()){ //表中存在供应商，则只更新合同号 hzg 2013-12-11
							record.put("GONGYFE", t_map.get("GONGYFE")); //GONGYFE不更新份额
							record.put("BIAOS", t_map.get("BIAOS")); //BIAOS不更新
							record.put("SHENGXSJ", t_map.get("SHENGXSJ")); //SHENGXSJ不更新
						}else{//插入一条记录，份额为0，BIAOS为0
							record.put("GONGYFE", ZERO_PERCENT);
							record.put("BIAOS", "0");
							record.put("SAP_FENE", ZERO_PERCENT);
						}
					}else {// 2、如果"用户中心"和"零件号"两个维度，并且当"合同号"不为空，而且”生效标识”为1(有效)  没有查到数据时   配额为100%,标识为1(有效)
						record.put("GONGYFE", HUNDRED_PERCENT);
						record.put("BIAOS", "1");
						record.put("SAP_FENE", HUNDRED_PERCENT);
					}
			}
			// 如果ZTYPE='2'时
			if ("2".equals(type)) {
				//当配额为0时标识为0(失效)，当配额非0时标识为1(生效)
				int i_GONGYFE=	Integer.parseInt(GONGYFE);
				if(i_GONGYFE==0){
					record.put("BIAOS","0");
				}else{
					record.put("BIAOS","1");
					//更新BIAOS为1 hzg 2014.11.25
					updateLinjgysBiaos(USERCENTER, LINGJBH, GONGYSBH);
				}
				
				// 根据"用户中心"、"零件号"、"供应商编码"三个维度去查询数据
				Map<String,String> t_map  = this.queryLingjGYS(USERCENTER, LINGJBH, GONGYSBH);
				if (!t_map.isEmpty()) {// 如果"用户中心"、"零件号"和"供应商编码"三个维度，查到数据
					record.put("GONGYHTH", t_map.get("GONGYHTH"));  //GONGYHTH合同号不更新,//GONGYFE更新份额,//BIAOS更新
					String a  =String.valueOf(i_edu==1.0?"1":i_edu);
					if(!a.equals(StrNull(t_map.get("SAP_FENE")))){ //本次sap下传的份额与数据库中sap的份额不相同
						record.put("FLAG", "1");
						record.put("SAP_FENE", i_edu);
						//写事务提醒表  hzg 2014.2.25
						String neirong = "份额从"+String.valueOf(t_map.get("SAP_FENE"))+"变为"+i_edu;
						insertShiwtx(USERCENTER, LINGJBH, GONGYSBH,neirong);
					}else{
						record.put("FLAG", StrNull(t_map.get("FLAG")));						
					}
				}
				
			}

			//获取供应商参考系表"发运地"到零件供应商参考系
			Map<String,String> map_fayd = this.queryFaydOfGongys(USERCENTER, GONGYSBH);
			if(!map_fayd.isEmpty()){
				record.put("FAYD", map_fayd.get("fayd"));
			}else{
				record.put("FAYD", "");
			}
		}catch(Exception e ){
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据SQL查询异常  " + e.getMessage());
			throw new ServiceException("SQL异常！"+e.getMessage());
		}

		record.put("CREATOR", interfaceId);
		record.put("CREATE_TIME", new Date());
		record.put("EDITOR", interfaceId);
		record.put("EDIT_TIME", new Date());
		return true;

	}
	
	
	/**
	 * TYPE=2 ，份额大于0时，更新零件供应商BIAOS=1
	 * @author 贺志国
	 * @date 2014-11-25
	 * @param USERCENTER
	 * @param LINGJBH
	 * @param GONGYSBH
	 */
	public void updateLinjgysBiaos(String USERCENTER,String LINGJBH,String GONGYSBH){
		StringBuilder updateBuf = new StringBuilder();
		updateBuf.append("update "+SpaceFinal.spacename_zbc+".CKX_LINGJGYS t set ");
		updateBuf.append(" biaos=1 ");
		updateBuf.append("where USERCENTER = '" + StrNull(USERCENTER) + "' and ");
		updateBuf.append("LINGJBH = '" + StrNull(LINGJBH) +"' and ");
		updateBuf.append("GONGYSBH = '" + StrNull(GONGYSBH)+"'");
		DbUtils.execute(updateBuf.toString(),connection);
	}
	
	/**
	 * 新增事务提醒
	 * @author 贺志国
	 * @date 2014-2-25
	 * @param usercenter 用户中心
	 * @param lingjbh 零件编号
	 * @param gongysbh 供应商编号
	 * @param neirong 内容
	 */
	public void insertShiwtx(String usercenter,String lingjbh,String gongysbh,String neirong){
		StringBuffer sqlbuf=new StringBuffer();
		sqlbuf.append("insert into "+SpaceFinal.spacename_zbc+".ckx_shiwtx(USERCENTER,TIXLX,GUANJZ1,GUANJZ2,YONGHZ,TIXNR,ZHUANGT,BAOJSJ,ID) values(");
		sqlbuf.append("'"+StrNull(usercenter)+"',");
		sqlbuf.append("'6',");
		sqlbuf.append("'"+StrNull(lingjbh)+"',");
		sqlbuf.append("'"+StrNull(gongysbh)+"',");
		sqlbuf.append("(select t.jihy from "+SpaceFinal.spacename_zbc+".ckx_lingj t ");
		sqlbuf.append("where t.usercenter = '"+StrNull(usercenter)+"' and t.lingjbh = '"+StrNull(lingjbh)+"'),");
		sqlbuf.append("'"+StrNull(neirong)+"',");
		sqlbuf.append("'0',");
		sqlbuf.append("sysdate,");
		sqlbuf.append("'"+UUIDHexGenerator.getInstance().generate()+"')");
		PreparedStatement ps =  null;
		try {
			ps = connection.prepareStatement(sqlbuf.toString());
			ps.executeUpdate();
		}  catch (SQLException e) {
			logger.error("线程--接口" + dataParserConfig.getId() +"新增ckx_shiwtx事务提醒表数据时报错，用户中心"+usercenter+"零件:"+lingjbh+"供应商:"+gongysbh+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"新增ckx_shiwtx事务提醒表数据时报错，用户中心"+usercenter+"零件:"+lingjbh+"供应商:"+gongysbh+e.getMessage());
		} finally{
			if(ps != null){
				try {
					ps.close();
				} catch (SQLException e) {
					logger.error("线程--接口" + dataParserConfig.getId() +"关闭连接错误"+e.getMessage(),e);
				}
			}
		}
	}
	
	/**
	 * 查询零件供应商信息
	 * @author 贺志国
	 * @date 2012-10-21
	 * @param USERCENTER 用户中心
	 * @param LINGJBH 零件编号
	 * @param GONGYSBH 供应商编号
	 * @return Map<String,String>零件供应商集合
	 */
	@SuppressWarnings("unchecked")
	public Map<String,String> queryLingjGYS(String USERCENTER,String LINGJBH,String GONGYSBH) throws ServiceException{
		/*Map<String,String> params = new HashMap<String,String>();
		params.put("usercenter", StrNull(USERCENTER));
		params.put("lingjbh", StrNull(LINGJBH));
		params.put("gongysbh", StrNull(GONGYSBH));
		List<Map<String,String>> gysList = dataParserConfig.getBaseDao().select("inPutzbc.queryLingjGYS",params);*/
		StringBuffer sqlBuf = new StringBuffer();
		sqlBuf.append("select t.gongyhth, t.gongyfe,t.shengxsj,t.biaos,t.sap_fene,t.flag"); 
		sqlBuf.append(" from "+SpaceFinal.spacename_zbc+".CKX_LINGJGYS t ");
		sqlBuf.append("where USERCENTER = '" + StrNull(USERCENTER) + "' and ");
		sqlBuf.append("LINGJBH = '" + StrNull(LINGJBH) +"' and ");
		sqlBuf.append("GONGYSBH = '" + StrNull(GONGYSBH) + "' and biaos='1'");
		Map<String,String> t_map = DbUtils.selectOne(sqlBuf.toString(),connection);
		return t_map;
		
	}
	
	/**
	 * 获取供应商参考系"发运地"
	 * @author 贺志国
	 * @date 2012-10-21
	 * @param USERCENTER 用户中心
	 * @param GONGYSBH 供应商编号
	 * @return Map<String,String> map集合
	 */
	@SuppressWarnings("unchecked")
	public Map<String,String> queryFaydOfGongys(String USERCENTER ,String GONGYSBH) throws ServiceException{
		/*Map<String,String> params = new HashMap<String,String>();
		params.put("usercenter", StrNull(USERCENTER));
		params.put("gongysbh", StrNull(GONGYSBH));
		List<Map<String,String>> fydList = dataParserConfig.getBaseDao().select("inPutzbc.queryFaydOfGongys",params);*/
		StringBuffer sqlBuf=new StringBuffer();
		sqlBuf.append("select t.fayd as fayd ");
		sqlBuf.append("from "+SpaceFinal.spacename_zbc+".ckx_gongys t ");
		sqlBuf.append("left join "+SpaceFinal.spacename_zbc+".ckx_lingjgys y ");
		sqlBuf.append("on y.usercenter = t.usercenter ");
		sqlBuf.append("and y.gongysbh = t.gcbh ");
		sqlBuf.append("where y.usercenter = '"+StrNull(USERCENTER)+"' and ");
		sqlBuf.append("y.gongysbh = '"+StrNull(GONGYSBH)+"'");
		Map<String,String> map_fayd = DbUtils.selectOne(sqlBuf.toString(),connection);
		return map_fayd;
	}
	
	/**
	 * 查询零件供应商判断份额
	 * @author 贺志国
	 * @date 2012-10-21
	 * @param USERCENTER 用户中心
	 * @param LINGJBH 零件编号
	 * @return Map<String,String> map集合
	 */
	@SuppressWarnings("unchecked")
	public Map<String,String> queryLingjGYSFE(String USERCENTER ,String LINGJBH) throws ServiceException{
		/*Map<String,String> params = new HashMap<String,String>();
		params.put("usercenter", StrNull(USERCENTER));
		params.put("lingjbh", StrNull(LINGJBH));
		List<Map<String,String>> feList = dataParserConfig.getBaseDao().select("inPutzbc.queryLingjGYSFE",params);*/
		StringBuffer strbuf = new StringBuffer();
		strbuf.append("select * ");
		strbuf.append("from "+SpaceFinal.spacename_zbc+".CKX_LINGJGYS ");
		strbuf.append("where USERCENTER = '" + StrNull(USERCENTER)+"' and ");
		strbuf.append("LINGJBH = '" + StrNull(LINGJBH) + "' and ");
		strbuf.append("GONGYHTH is not null and ");
		strbuf.append("BIAOS = '1'");
		Map<String,String> y_map = DbUtils.selectOne(strbuf.toString(),connection);
		return y_map;
	}


	/**
	 * 判断字符为空时
	 * @param objstr
	 * @return
	 */
	private String StrNull(Object objstr){
		return objstr==null?"":objstr.toString();
	}
}
