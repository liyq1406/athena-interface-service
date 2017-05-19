package exchange;





import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.athena.component.exchange.DataExchange;
import com.athena.component.service.imp.InterfaceServiceImp;
import com.athena.component.test.AbstractCompomentTests;
import com.toft.core3.container.annotation.Inject;
		
public class DataExchangeTests  extends AbstractCompomentTests{
	
		@Inject
		private DataExchange dataEchange;
		
		@Inject
		private InterfaceServiceImp interImp;
		
		//@Test
		public void doExchange()
		{		
           Date date=new Date();
           long begintime=date.getTime();
           
		   dataEchange.doExchange("1010","in");
		   
		   
		   Date b_date=new Date();
		   long endtime=b_date.getTime();
		   long time=endtime-begintime;
		   System.out.println(time/1000+"秒");
		}
		
		/**
		 * 测试DEDIPP xqjs_diaobsq
		 * @author 贺志国
		 * @date 2013-10-12
		 */
//		@Test
//		public void testGetDbsq(){
//			List<Map<String,String>> list  = interImp.GetDbsq("UW", "13000031");
//			if(list.size()>0&&"00".equals(list.get(0).get("ZHUANGT"))){
//				System.out.println(""+list.get(0).get("ZHUANGT"));
//				assertEquals("00",list.get(0).get("ZHUANGT"));
//				assertEquals("13000034",list.get(0).get("DIAOBSQDH"));
//			}else{
//				if(list.size()==0){
//					System.out.println("list is null");
//				}else{
//					System.out.println("list is not null and ZHUANGT is 30");
//				}
//			}
//		}
		
		/**
		 * 测试DEDIPP xqjs_diaobsqmx
		 * @author 贺志国
		 * @date 2013-10-12
		 */
//		@Test
//		public void testGetDbsqdmx(){
//			List<Map<String,String>> list  = interImp.GetDbsqdmx( "UW","13000027","97V","9614181580");
//			if(list.size()>0&&"00".equals(list.get(0).get("ZHUANGT"))){
//				System.out.println(""+list.get(0).get("ZHUANGT"));
//				assertEquals("00",list.get(0).get("ZHUANGT"));
//				assertEquals("13000027",list.get(0).get("DIAOBSQDH"));
//			}else{
//				if(list.size()==0){
//					System.out.println("list is null");
//				}else{
//					System.out.println("list is not null and ZHUANGT is 30");
//				}
//			}
//		}
}






