package com.athena.component.service.bean;

public class ServiceBean {

	private String dbsqdh; //调拨申请单号
	
	private String dbsq_date; //调拨申请时间
	
	private String kjkm;//会计科目

	private String cbzx;//成本中心
	
	private String zzlx;//制造路线
	
	private String ljh;//零件号

	private String ljhmc;//零件号名称
	
    private double sbsl;//申报数量
	
	private String jldw;//计量单位

	private String usercenter;//用户中心
	
	private String operator ; //操作人 add hzg 14.5.13
	
	private String beiz;   //备注  add hzg 16.10.27
	
	public String getBeiz() {
		return beiz;
	}

	public void setBeiz(String beiz) {
		this.beiz = beiz;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public String getLjhmc() {
		return ljhmc;
	}

	public void setLjhmc(String ljhmc) {
		this.ljhmc = ljhmc;
	}

	public String getJldw() {
		return jldw;
	}

	public void setJldw(String jldw) {
		this.jldw = jldw;
	}

	

	public String getDbsqdh() {
		return dbsqdh;
	}

	public void setDbsqdh(String dbsqdh) {
		this.dbsqdh = dbsqdh;
	}

	public String getDbsq_date() {
		return dbsq_date;
	}

	public void setDbsq_date(String dbsq_date) {
		this.dbsq_date = dbsq_date;
	}

	public String getKjkm() {
		return kjkm;
	}

	public void setKjkm(String kjkm) {
		this.kjkm = kjkm;
	}

	public String getCbzx() {
		return cbzx;
	}

	public void setCbzx(String cbzx) {
		this.cbzx = cbzx;
	}

	public String getZzlx() {
		return zzlx;
	}

	public void setZzlx(String zzlx) {
		this.zzlx = zzlx;
	}

	public String getLjh() {
		return ljh;
	}

	public void setLjh(String ljh) {
		this.ljh = ljh;
	}

	public double getSbsl() {
		return sbsl;
	}

	public void setSbsl(double sbsl) {
		this.sbsl = sbsl;
	}


	public String getUsercenter() {
		return usercenter;
	}

	public void setUsercenter(String usercenter) {
		this.usercenter = usercenter;
	}
	

}
