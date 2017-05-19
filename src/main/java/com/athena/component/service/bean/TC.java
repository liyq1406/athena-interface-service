/**
 * 
 */
package com.athena.component.service.bean;

import com.toft.core3.support.PageableSupport;

/**
 * 拉箱开箱 TC集装箱
 * 
 * @author dsimedd001
 * 
 */
public class TC extends PageableSupport {
	private String id;// id
	private String tcNo;// TC号
	private String utNo;// UT号
	private String qiysj;// 启运时间
	private String qiyd;// 启运点
	private String mudd;// 目的地
	private String dinghcj;// 订货车间
	private String yujddsj;// 预计到达神龙时间
	private String zuiswld;// 最新物理点
	private String daodwldsj;// 到达物理点时间
	private String zuixyjddsj;// 最新预计到达时间
	private String laxzdddsj;// 拉箱指定到达时间
	private String kaixzdsj;// 开箱指定时间
	private String tczt;// TC状态
	private String creator;//创建人
	private String createTime;//创建时间
	private String editor;//修改人
	private String editTime;//修改时间
	private String lujdm;
	private String papSheetId;
	private String papBoxId;
	private String kdysSheetId;
	private String xiangx; //新增字段 箱型
	private String guanxsj;// 关箱时间
	
	
	
	public String getGuanxsj() {
		return guanxsj;
	}

	public void setGuanxsj(String guanxsj) {
		this.guanxsj = guanxsj;
	}

	public String getXiangx() {
		return xiangx;
	}

	public void setXiangx(String xiangx) {
		this.xiangx = xiangx;
	}

	/**
	 * 取得 id
	 * 
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * 取得 tcNo
	 * 
	 * @return the tcNo
	 */
	public String getTcNo() {
		return tcNo;
	}

	/**
	 * @param tcNo
	 *            the tcNo to set
	 */
	public void setTcNo(String tcNo) {
		this.tcNo = tcNo;
	}

	/**
	 * 取得 utNo
	 * 
	 * @return the utNo
	 */
	public String getUtNo() {
		return utNo;
	}

	/**
	 * @param utNo
	 *            the utNo to set
	 */
	public void setUtNo(String utNo) {
		this.utNo = utNo;
	}

	/**
	 * 取得 qiysj
	 * 
	 * @return the qiysj
	 */
	public String getQiysj() {
		return qiysj;
	}

	/**
	 * @param qiysj
	 *            the qiysj to set
	 */
	public void setQiysj(String qiysj) {
		this.qiysj = qiysj;
	}

	/**
	 * 取得 qiyd
	 * 
	 * @return the qiyd
	 */
	public String getQiyd() {
		return qiyd;
	}

	/**
	 * @param qiyd
	 *            the qiyd to set
	 */
	public void setQiyd(String qiyd) {
		this.qiyd = qiyd;
	}

	/**
	 * 取得 mudd
	 * 
	 * @return the mudd
	 */
	public String getMudd() {
		return mudd;
	}

	/**
	 * @param mudd
	 *            the mudd to set
	 */
	public void setMudd(String mudd) {
		this.mudd = mudd;
	}

	/**
	 * 取得 dinghcj
	 * 
	 * @return the dinghcj
	 */
	public String getDinghcj() {
		return dinghcj;
	}

	/**
	 * @param dinghcj
	 *            the dinghcj to set
	 */
	public void setDinghcj(String dinghcj) {
		this.dinghcj = dinghcj;
	}

	/**
	 * 取得 yujddsj
	 * 
	 * @return the yujddsj
	 */
	public String getYujddsj() {
		return yujddsj;
	}

	/**
	 * @param yujddsj
	 *            the yujddsj to set
	 */
	public void setYujddsj(String yujddsj) {
		this.yujddsj = yujddsj;
	}

	/**
	 * 取得 zuiswld
	 * 
	 * @return the zuiswld
	 */
	public String getZuiswld() {
		return zuiswld;
	}

	/**
	 * @param zuiswld
	 *            the zuiswld to set
	 */
	public void setZuiswld(String zuiswld) {
		this.zuiswld = zuiswld;
	}

	/**
	 * 取得 daodwldsj
	 * 
	 * @return the daodwldsj
	 */
	public String getDaodwldsj() {
		return daodwldsj;
	}

	/**
	 * @param daodwldsj
	 *            the daodwldsj to set
	 */
	public void setDaodwldsj(String daodwldsj) {
		this.daodwldsj = daodwldsj;
	}

	/**
	 * 取得 zuixyjddsj
	 * 
	 * @return the zuixyjddsj
	 */
	public String getZuixyjddsj() {
		return zuixyjddsj;
	}

	/**
	 * @param zuixyjddsj
	 *            the zuixyjddsj to set
	 */
	public void setZuixyjddsj(String zuixyjddsj) {
		this.zuixyjddsj = zuixyjddsj;
	}

	/**
	 * 取得 laxzdddsj
	 * 
	 * @return the laxzdddsj
	 */
	public String getLaxzdddsj() {
		return laxzdddsj;
	}

	/**
	 * @param laxzdddsj
	 *            the laxzdddsj to set
	 */
	public void setLaxzdddsj(String laxzdddsj) {
		this.laxzdddsj = laxzdddsj;
	}

	/**
	 * 取得 kaixzdsj
	 * 
	 * @return the kaixzdsj
	 */
	public String getKaixzdsj() {
		return kaixzdsj;
	}

	/**
	 * @param kaixzdsj
	 *            the kaixzdsj to set
	 */
	public void setKaixzdsj(String kaixzdsj) {
		this.kaixzdsj = kaixzdsj;
	}

	/**
	 * 取得 tczt
	 * 
	 * @return the tczt
	 */
	public String getTczt() {
		return tczt;
	}

	/**
	 * @param tczt
	 *            the tczt to set
	 */
	public void setTczt(String tczt) {
		this.tczt = tczt;
	}

	/**
	 * 取得 creator
	 * @return the creator
	 */
	public String getCreator() {
		return creator;
	}

	/**
	 * @param creator the creator to set
	 */
	public void setCreator(String creator) {
		this.creator = creator;
	}

	/**
	 * 取得 createTime
	 * @return the createTime
	 */
	public String getCreateTime() {
		return createTime;
	}

	/**
	 * @param createTime the createTime to set
	 */
	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	/**
	 * 取得 editor
	 * @return the editor
	 */
	public String getEditor() {
		return editor;
	}

	/**
	 * @param editor the editor to set
	 */
	public void setEditor(String editor) {
		this.editor = editor;
	}

	/**
	 * 取得 editTime
	 * @return the editTime
	 */
	public String getEditTime() {
		return editTime;
	}

	/**
	 * @param editTime the editTime to set
	 */
	public void setEditTime(String editTime) {
		this.editTime = editTime;
	}

	/**
	 * 取得 lujdm
	 * @return the lujdm
	 */
	public String getLujdm() {
		return lujdm;
	}

	/**
	 * @param lujdm the lujdm to set
	 */
	public void setLujdm(String lujdm) {
		this.lujdm = lujdm;
	}

	/**
	 * 取得 papSheetId
	 * @return the papSheetId
	 */
	public String getPapSheetId() {
		return papSheetId;
	}

	/**
	 * @param papSheetId the papSheetId to set
	 */
	public void setPapSheetId(String papSheetId) {
		this.papSheetId = papSheetId;
	}

	/**
	 * 取得 papBoxId
	 * @return the papBoxId
	 */
	public String getPapBoxId() {
		return papBoxId;
	}

	/**
	 * @param papBoxId the papBoxId to set
	 */
	public void setPapBoxId(String papBoxId) {
		this.papBoxId = papBoxId;
	}

	/**
	 * 取得 kdysSheetId
	 * @return the kdysSheetId
	 */
	public String getKdysSheetId() {
		return kdysSheetId;
	}

	/**
	 * @param kdysSheetId the kdysSheetId to set
	 */
	public void setKdysSheetId(String kdysSheetId) {
		this.kdysSheetId = kdysSheetId;
	}
}
