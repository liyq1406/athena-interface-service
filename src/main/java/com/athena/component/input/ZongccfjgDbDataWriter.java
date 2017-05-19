package com.athena.component.input;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.apache.log4j.Logger;

import com.athena.component.DateTimeUtil;
import com.athena.component.exchange.Record;
import com.athena.component.exchange.SpaceFinal;
import com.athena.component.exchange.config.DataParserConfig;
import com.athena.component.exchange.db.TableRecord;
import com.athena.component.exchange.txt.TxtWriterDBTask;
import com.athena.component.exchange.txt.WarmBusinessException;
import com.athena.util.exception.ServiceException;


/**
 * 3300一行多个日期处理
 * @author yz
 * @date 2016-3-9
 */
public class ZongccfjgDbDataWriter extends TxtWriterDBTask {
	protected static Logger logger = Logger.getLogger(ZongccfjgDbDataWriter.class);	//定义日志方法 
	//private  String datasourceId = "";
	 
	public ZongccfjgDbDataWriter(DataParserConfig dataParserConfig, List<String> fieldList, List<String> updateFieldList, String fileName, List<Record> lineList) {
		super(dataParserConfig, fieldList, updateFieldList, fileName, lineList); 
		datasourceId = dataParserConfig.getWriterConfig().getDatasourceId();
	}

	/**
	 * 解析后处理方法
	 * hzg 2013-4-2
	 */
	@Override
	public boolean beforeRecord(Record record) {
		try {
				//业务处理 
				logger.info("线程--接口" + interfaceId + "文件" + fileName + "解析第" + record.getLineNum() + "行数据  进入beforeRecord");
				String chej = record.getString("chej");//车间
				String usercenter = chej.substring(0,2);//用户中心
				String zongch = record.getString("zongch");//总成号
				String lingj = record.getString("lingj");  //零件号
				String xiaohd = record.getString("xiaohd"); //消耗点
				String danw = record.getString("danw");  //单位		
				String xiaohdxs = record.getString("xis");//消耗点系数
				double xis = 0;
				try {
					 xis = Double.parseDouble(xiaohdxs);
				} catch (NumberFormatException e) {
					logger.error("消耗点系数为空，"+e.getMessage());
					 throw new WarmBusinessException("消耗点系数为空"+e.getMessage());
				}
				String EMON = record.getParam().get("emon").toString(); //EMON商业化时间
				String[] emon = EMON.split(",");
				for(int y=0;y<emon.length;y++){
					String kaiszkrq="";
					String jieszkrq="";
					String[] dataStr=null;
					if(emon[y].indexOf("-")!=-1){
						//如果时间为20120501-20120502
						dataStr = emon[y].split("-");
						kaiszkrq = DateTimeUtil.DateStr(dataStr[0]);
						jieszkrq = DateTimeUtil.DateStr(dataStr[1]);
						//logger.info("线程--接口" + interfaceId + "文件" + fileName + "解析第" + record.getLineNum() + "行数据   解析日期" + EMON);
						AddTable(usercenter,chej,zongch,lingj,xiaohd,danw,xis,kaiszkrq,jieszkrq);
						dataParserConfig.getInsertCount();
					}else{
						//没有-表示开始时间和结束时间相同
						kaiszkrq = DateTimeUtil.DateStr(emon[y].toString());
						AddTable(usercenter,chej,zongch,lingj,xiaohd,danw,xis,kaiszkrq,kaiszkrq);
						dataParserConfig.getInsertCount();
					}
				}
		}catch(RuntimeException e){
			dataParserConfig.getErrorCount();
			logger.error("线程--接口" + interfaceId + "第" + record.getLineNum() + "行数据错误  "+record.getString("lingj") + e.getMessage());
		}
		return true;
	}


	@Override
	public void exec(TableRecord tableReocrd, Record record) throws SQLException {
		
	}
	
	/**
	 * 新增in_zongccfjg表数据
	 * @param dataStr
	 * @param xhdxs
	 * @throws Exception 
	 * @throws SQLException 
	 */
	public void AddTable(String usercenter,String chej,String zongch,String lingj,String xiaohd,String danw,double xis,
			String kaiszkrq,String jieszkrq){
		StringBuffer sqlbuf=new StringBuffer();
		sqlbuf.append("insert into "+SpaceFinal.spacename_ddbh+".in_zongccfjg (USERCENTER,CHEJ,ZONGCH,LINGJ,XIAOHD,DANW,XIS,KAISZKRQ,JIESZKRQ,CREATOR,CREATE_TIME,EDITOR,EDIT_TIME) values(");
		sqlbuf.append("'"+usercenter+"',");
		sqlbuf.append("'"+chej+"',");
		sqlbuf.append("'"+zongch+"',");
		sqlbuf.append("'"+lingj+"',");
		sqlbuf.append("'"+xiaohd+"',");
		sqlbuf.append("'"+danw+"',");
		sqlbuf.append(""+xis+",");
		sqlbuf.append("'"+strNull(kaiszkrq)+"',");
		sqlbuf.append("'"+strNull(jieszkrq)+"',"); 
		sqlbuf.append("'3300',");
		sqlbuf.append("sysdate,");
		sqlbuf.append("'3300',");
		sqlbuf.append("sysdate)");
		PreparedStatement ps =  null;
		try {
			ps = connection.prepareStatement(sqlbuf.toString());
			ps.executeUpdate();
		}  catch (SQLException e) {
			logger.error("线程--接口" + dataParserConfig.getId() +"插入in_zongccfjg表时报错"+e.getMessage());
			throw new ServiceException("线程--接口" + dataParserConfig.getId() +"插入in_zongccfjg表时报错"+e.getMessage());
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
	 * 空串处理
	 * 
	 * @param obj
	 *            对象
	 * @return 处理后字符串
	 * @author GJ
	 * @date 2011-10-26
	 */
	private String strNull(Object obj) {// 对象为空返回空串,不为空toString
		return obj == null ? "" : obj.toString().trim();
	}

}
