package com.athena.component;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.athena.component.service.SysmonitorService;
import com.athena.component.utils.LoaderProperties;
import com.athena.truck.component.QueueTimeController;
import com.toft.core3.web.context.support.WebSdcContextUtils;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import org.apache.log4j.Logger;
/**
 * 车辆入厂定时排队及分配车位程序
 * @author xiah
 */

public class KacTimerListener implements ServletContextListener{

  ServletContext servletContext;
//配置文件路径
 ScheduledExecutorService schedule = Executors.newSingleThreadScheduledExecutor();

  private final String fileName="config/exchange/urlPath.properties"; 
  String kacTimer_builder=LoaderProperties.getPropertiesMap(fileName).get("kacTimer_builder");
 
  private static Logger logger=Logger.getLogger(KacTimerListener.class);
  public void contextInitialized(ServletContextEvent event){
	   
    this.servletContext = event.getServletContext();
    if(kacTimer_builder.equals("ZXC")){
    	
		    schedule.scheduleWithFixedDelay(new Runnable() {
		         public void run() {
		             try {
		              logger.info("车辆排队管理定时运行："+System.currentTimeMillis());
		              QueueTimeController controller = WebSdcContextUtils.getWebSdcContext(servletContext).getComponent(QueueTimeController.class);
		              controller.run();
		            } catch (Exception e){
		               logger.error("排队队列管理出现异常");
		               logger.error(e);
		               String taskName="卡车入厂排队批量";
		               logger.error("卡车:"+taskName+"出现异常，报集中监控平台处理......"+e.getMessage(),e);
						sendKacAlarm(taskName,e);
		            }
		        }
		   },5 ,1, TimeUnit.MINUTES);  
	   }

  }
  public void contextDestroyed(ServletContextEvent event){
	  
    schedule.shutdown();
    WebSdcContextUtils.destroyWebSdcContext(this.servletContext);
    logger.info(" 车辆排队定时器销毁！");
  }
  
  
  /**
   * 判断是否是卡车批量异常，如果是则向集中监控平台发送报警
   * @author xh
   * @date 2015-7-7
   * @param taskName
   * @param e
   */
  public void sendKacAlarm(String taskName,Exception e){
  	//发送报警 1.数据库异常
  	try {
  		SysmonitorService service = new SysmonitorService();
  		if(e.getMessage().contains("SQL")||e.getMessage().contains("JDBC")){
  			service.SendAlert("", "0005", "3", "008", taskName+" Database connection failed. ");
  			logger.info("####卡车入厂数据库操作异常："+e.getMessage());
  		}else{//2.业务处理异常
  			service.SendAlert("", "0003", "3", "008", taskName+" Business operate error. ");
  			logger.info("@@@@卡车入厂业务操作异常："+e.getMessage());
  		}
  	}catch (Exception e1) {
  		  logger.error(taskName+"==>>发送报警出错：" + e1.getMessage());
  	}
  }
  
}

