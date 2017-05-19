package com.athena.component.service.bean;

/**
 * @author 
 *
 */
public class Yaohl {
	private String uth;			//UT号
	private String yaohlh;		//要货令号
	private String fahd;		//发货地
	private String cangkbh;		//仓库编号
	private String zuiwsj;		//最晚时间
	private String usercenter;	//用户中心
	private String yaohlzt;		//要货令状态
	private String shijfysj; 	//实际发运时间，由KDYS更新
	private String xiughyjjfsj;	//修改货源交付时间，由KDYS更新
	private String jiaofzt;		//由KDYS更新，0：延迟，1：提前，2：正常
	private String dinghcj;		//订货车间
	private String lujdm;		//路径代码
	
	public String getUth() {
		return uth;
	}
	public void setUth(String uth) {
		this.uth = uth;
	}
	public String getYaohlh() {
		return yaohlh;
	}
	public void setYaohlh(String yaohlh) {
		this.yaohlh = yaohlh;
	}
	public String getFahd() {
		return fahd;
	}
	public void setFahd(String fahd) {
		this.fahd = fahd;
	}
	public String getCangkbh() {
		return cangkbh;
	}
	public void setCangkbh(String cangkbh) {
		this.cangkbh = cangkbh;
	}
	public String getZuiwsj() {
		return zuiwsj;
	}
	public void setZuiwsj(String zuiwsj) {
		this.zuiwsj = zuiwsj;
	}
	public String getUsercenter() {
		return usercenter;
	}
	public void setUsercenter(String usercenter) {
		this.usercenter = usercenter;
	}
	public String getYaohlzt() {
		return yaohlzt;
	}
	public void setYaohlzt(String yaohlzt) {
		this.yaohlzt = yaohlzt;
	}
	public String getShijfysj() {
		return shijfysj;
	}
	public void setShijfysj(String shijfysj) {
		this.shijfysj = shijfysj;
	}
	public String getXiughyjjfsj() {
		return xiughyjjfsj;
	}
	public void setXiughyjjfsj(String xiughyjjfsj) {
		this.xiughyjjfsj = xiughyjjfsj;
	}
	public String getJiaofzt() {
		return jiaofzt;
	}
	public void setJiaofzt(String jiaofzt) {
		this.jiaofzt = jiaofzt;
	}
	public String getDinghcj() {
		return dinghcj;
	}
	public void setDinghcj(String dinghcj) {
		this.dinghcj = dinghcj;
	}
	public String getLujdm() {
		return lujdm;
	}
	public void setLujdm(String lujdm) {
		this.lujdm = lujdm;
	}
}
