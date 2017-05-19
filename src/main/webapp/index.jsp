<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>测试</title>

<script type="text/javascript">
	//提交给后台
	function goto_Inter(val){
		var formEle = document.getElementById("form_id");
		var hidden_input = document.getElementById("interFaceType");
		hidden_input.value = val;
		formEle.submit();
	}
	//输入框测试
	function goto_Inter1(){
		var formEle = document.getElementById("form_id");
		var hidden_input = document.getElementById("interFaceType");
		hidden_input.value = document.getElementById("zz").value;
		formEle.submit();
	}
	
</script>
</head>
<body>
	<form action="${pageContext.request.contextPath }/test" method="post" id="form_id">
		<input type="hidden" name="interFaceType" id ="interFaceType" >
    	
    	<table>
    	   	<tr>
    			<td ><font color="red" size="5">输出接口：</font></td>
    		</tr>
    		<tr>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1100)"  value="【1100】看板循环规模" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1110)"  value="【1110】工艺消耗点" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1120)"  value="【1120】零件消耗点" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1130)"  value="【1130】未来几日" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1140)"  value="【1140】生产线" width="35"/></td> 
			</tr>
			<tr>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1180)"  value="【1180】供承运" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1190)"  value="【1190】零件" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1200)"  value="【1200】零件仓库" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1210)"  value="【1210】卸货站台" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1230)"  value="【1230】订单明细" width="35"/></td>
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1240)"  value="【1240】物流路径总图" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1250)"  value="【1250】零件供应商" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1260)"  value="【1260】运输时刻" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1270)"  value="【1270】外部物流" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1280)"  value="【1280】仓库循环时间" width="35"/></td>
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1290)"  value="【1290】调拨申请" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1300)"  value="【1300】调拨明细" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1310)"  value="【1310】运输物理点" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1330)"  value="【1330】包装" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1340)"  value="【1340】配送类别" width="35"/></td>
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1350)"  value="【1350】日历版次" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1360)"  value="【1360】工作时间" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1370)"  value="【1370】工作时间编组" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1430)"  value="【1430】小火车运输时刻表" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1440)"  value="【1440】生产线-线边仓库" width="35"/></td>
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1450)"  value="【1450】变更记录" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1460)"  value="【1460】订单" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1470)"  value="【1470】下游输出接口：零件" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1480)"  value="【1480】供应商" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1490)"  value="【1490】零件供应商" width="35"/></td>
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1500)"  value="【1500】生产日历" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1510)"  value="【1510】周期订单-EFI" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1520)"  value="【1520】预告-订单明细" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1530)"  value="【1530】预批量零件需求" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1540)"  value="【1540】资源文件" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1550)"  value="【1550】供货路线" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1560)"  value="【1560】(需拆分的)零件消耗点" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1570)"  value="【1570】循环对产线" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1580)"  value="【1580】包装" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2130)"  value="【2130】异常申报（零件消耗表）" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2280)"  value="【2280】集装箱（TC）" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2350)"  value="【2350】内部要货令(输出到执行层)" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2360)"  value="【2360】到货通知单" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2370)"  value="【2370】US标签" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2380)"  value="【2380】外部要货令" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2440)"  value="【2440】成品库存快照" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2450)"  value="【2450】零件库存快照" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2470)"  value="【2470】成品库流水账" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2490)"  value="【2490】入库反馈" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2500)"  value="【2500】发货通知信息" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(3110)"  value="【3110】DDBH拆分结果" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2120)"  value="【2120】库存快照" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2550)"  value="【2550】订单" width="35"/></td> 
       			 <td><input type="button" name="jiek_1" onclick="goto_Inter(1630)"  value="【1630】车型平台" width="35"/></td>
       			 <td><input type="button" name="jiek_1" onclick="goto_Inter(2521)"  value="【2521】EFI同步要货令" width="35"/></td>
       		</tr>
       		<tr><td></td><td></td>
       			<td>
			    	<h2>
				    	<font color="red" >请输入接口ID：</font>
				   		<input type="text" name="zz" id="zz"/>
				   		<input type="button" id="_button" value="确认" onclick="goto_Inter1()"/>
			    	</h2>
	    		</td>
    		</tr>
       		<tr>
    			<td ><font color="red" size="5">输入接口：</font></td>
    		</tr>
    		<tr>
       			 <td><input type="button" name="jiek_1" onclick="goto_Inter(1010)"  value="【1010】零件MAF库存" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1020)"  value="【1020】订单信息" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1040)"  value="【1040】零件参考系" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1050)"  value="【1050】零件周期毛需求" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1051)"  value="【1051】KD件零件周期毛需求" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1052)"  value="【1052】零件日毛需求" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1053)"  value="【1053】襄阳总成排产" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1060)"  value="【1060】供应商参考系" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1070)"  value="【1070】零件供应商参考系" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1080)"  value="【1080】消耗点参考系" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1090)"  value="【1090】零件消耗点参考系" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1150)"  value="【1150】DDBH拆分结果" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1160)"  value="【1160】库存快照" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1170)"  value="【1170】异常申报" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1320)"  value="【1320】集装箱（TC）" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1380)"  value="【1380】到货通知单" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1390)"  value="【1390】US标签" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1400)"  value="【1400】外部要货令" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1410)"  value="【1410】内部要货令" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(1420)"  value="【1420】出库明细" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2040)"  value="【2040】EFI发货通知" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2050)"  value="【2050】EDI发货通知" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2010)"  value="【2010】外部订单预告" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2140)"  value="【2140】供应商-承运商-运输商" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2150)"  value="【2150】零件" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2160)"  value="【2160】零件仓库设置" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2170)"  value="【2170】卸货站台" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2180)"  value="【2180】订单零件" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2190)"  value="【2190】订单明细" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2220)"  value="【2220】运输时刻" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2230)"  value="【2230】外部物流路径" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2240)"  value="【2240】仓库循环时间" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2250)"  value="【2250】调拨申请" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2260)"  value="【2260】调拨明细" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2270)"  value="【2270】运输物理点" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2290)"  value="【2290】包装" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2300)"  value="【2300】配送类别" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2320)"  value="【2320】工作时间" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2330)"  value="【2330】工作时间 编组" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2390)"  value="【2390】小火车运输时刻表" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2400)"  value="【2400】生产线-线边仓库" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2410)"  value="【2410】变更记录" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2420)"  value="【2420】订单" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(3010)"  value="【3010】商业OF接口" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(3020)"  value="【3020】整车过点信息" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(3030)"  value="【3030】CODDC-消耗点零件" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(3040)"  value="【3040】CLDV-CODDC对应关系" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(3050)"  value="【3050】九天排产计划(商业化的时间)" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(3060)"  value="【3060】九天排产计划(JT的顺序)" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(3070)"  value="【3070】工艺消耗点" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(3080)"  value="【3080】零件-消耗点" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(3090)"  value="【3090】未来几日剔除休息时间" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(3100)"  value="【3100】生产线" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2020)"  value="【2020】过点同步需求" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2030)"  value="【2030】GEVP外部要货令" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2060)"  value="【2060】KD件在途物理点信息" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2070)"  value="【2070】看板循环规模" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2080)"  value="【2080】工艺消耗点" width="35"/></td> 
			</tr>
			<tr>	  
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2090)"  value="【2090】零件-消耗点" width="35"/></td> 
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2100)"  value="【2100】未来几日剔除休息时间" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(2110)"  value="【2110】生产线" width="35"/></td>
				 <td><input type="button" name="jiek_1" onclick="goto_Inter(3120)"  value="【3120】车型平台" width="35"/></td> 
    		</tr>
       		
    	</table>
    	
    </form>
</body>
</html>