package com.athena.component.service.bean;

import java.math.BigDecimal;

/**
 * 
 * @author 贺志国
 * @E-mail zghe@isoftstone.com
 * @version v1.0
 * @date 2015-7-23
 */
public class Lingjck {
	private String usercenter; //用户中心
	private String lingjbh;  //零件编号
	private String cangkbh;  //仓库编号
	private String usbzlx; //US包装类型
	private BigDecimal usbzrl;//US包装容量
	private String uclx; //UC类型
	private BigDecimal ucrl;//UC容量
	
	public String getUsercenter() {
		return usercenter;
	}
	public void setUsercenter(String usercenter) {
		this.usercenter = usercenter;
	}
	public String getLingjbh() {
		return lingjbh;
	}
	public void setLingjbh(String lingjbh) {
		this.lingjbh = lingjbh;
	}
	public String getCangkbh() {
		return cangkbh;
	}
	public void setCangkbh(String cangkbh) {
		this.cangkbh = cangkbh;
	}
	public String getUsbzlx() {
		return usbzlx;
	}
	public void setUsbzlx(String usbzlx) {
		this.usbzlx = usbzlx;
	}
	public BigDecimal getUsbzrl() {
		return usbzrl;
	}
	public void setUsbzrl(BigDecimal usbzrl) {
		this.usbzrl = usbzrl;
	}
	public String getUclx() {
		return uclx;
	}
	public void setUclx(String uclx) {
		this.uclx = uclx;
	}
	public BigDecimal getUcrl() {
		return ucrl;
	}
	public void setUcrl(BigDecimal ucrl) {
		this.ucrl = ucrl;
	}
	
	
}
