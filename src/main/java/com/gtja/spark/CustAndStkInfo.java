/*
 * ================================================================
 * Copyright 2008-2015 AMT.
 * All Rights Reserved.
 *
 * This software is the confidential and proprietary information of
 * AMT Corp. Ltd, ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with AMT.
 * 
 * 国泰君安智能投顾项目	
 *
 * ================================================================
 *  创建人: lipeipei
 *	创建时间: 2017年12月19日 - 上午10:20:12
 */
package com.gtja.spark;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * <p>
 * 为了计算客户打分的临时数据实体类
 * </p>
 *
 * @author lipeipei
 *
 * @version 1.0.0
 *
 * @since 1.0.0
 *
 */
public class CustAndStkInfo implements Serializable {

	private int customer_id;
	private int stock_id;
	private BigDecimal hold_asset_avg;
	private int hold_days;
	private int match_cnt;
	private int in_match_lastdate;
	private BigDecimal hold_asset;
	private int total_match_cnt;
	private int total_hold_days;


	/**
	 * @return the customer_id
	 */
	public int getCustomer_id() {
		return customer_id;
	}
	/**
	 * @param customer_id the customer_id to set
	 */
	public void setCustomer_id(int customer_id) {
		this.customer_id = customer_id;
	}
	/**
	 * @return the stock_id
	 */
	public int getStock_id() {
		return stock_id;
	}
	/**
	 * @param stock_id the stock_id to set
	 */
	public void setStock_id(int stock_id) {
		this.stock_id = stock_id;
	}
	/**
	 * @return the hold_asset_avg
	 */
	public BigDecimal getHold_asset_avg() {
		return hold_asset_avg;
	}
	/**
	 * @param hold_asset_avg the hold_asset_avg to set
	 */
	public void setHold_asset_avg(BigDecimal hold_asset_avg) {
		if(hold_asset_avg == null )
			hold_asset_avg = new BigDecimal(0);
		else
			this.hold_asset_avg = hold_asset_avg;
	}
	/**
	 * @return the hold_days
	 */
	public int getHold_days() {
		return hold_days;
	}
	/**
	 * @param hold_days the hold_days to set
	 */
	public void setHold_days(int hold_days) {
		this.hold_days = hold_days;
	}
	/**
	 * @return the match_cnt
	 */
	public int getMatch_cnt() {
		return match_cnt;
	}
	/**
	 * @param match_cnt the match_cnt to set
	 */
	public void setMatch_cnt(int match_cnt) {
		this.match_cnt = match_cnt;
	}
	/**
	 * @return the in_match_lastdate
	 */
	public int getIn_match_lastdate() {
		return in_match_lastdate;
	}
	/**
	 * @param in_match_lastdate the in_match_lastdate to set
	 */
	public void setIn_match_lastdate(int in_match_lastdate) {
		this.in_match_lastdate = in_match_lastdate;
	}
	/**
	 * @return the hold_asset
	 */
	public BigDecimal getHold_asset() {
		return hold_asset;
	}
	/**
	 * @param hold_asset the hold_asset to set
	 */
	public void setHold_asset(BigDecimal hold_asset) {
		if(hold_asset == null )
			hold_asset = new BigDecimal(0);
		else
			this.hold_asset = hold_asset;
	}
	/**
	 * @return the total_match_cnt
	 */
	public int getTotal_match_cnt() {
		return total_match_cnt;
	}
	/**
	 * @param total_match_cnt the total_match_cnt to set
	 */
	public void setTotal_match_cnt(int total_match_cnt) {
		this.total_match_cnt = total_match_cnt;
	}
	/**
	 * @return the total_hold_days
	 */
	public int getTotal_hold_days() {
		return total_hold_days;
	}
	/**
	 * @param total_hold_days the total_hold_days to set
	 */
	public void setTotal_hold_days(int total_hold_days) {
		this.total_hold_days = total_hold_days;
	}

	/**
	 * @param customer_no
	 * @param stock_code
	 * @param hold_asset_avg
	 * @param hold_days
	 * @param match_cnt
	 * @param in_match_lastdate
	 * @param hold_asset
	 * @param total_match_cnt
	 * @param total_hold_days
	 */
	public CustAndStkInfo(int customer_id, int stock_id, BigDecimal hold_asset_avg, int hold_days,
						  int match_cnt, int in_match_lastdate, BigDecimal hold_asset, int total_match_cnt, int total_hold_days) {
		super();
		this.customer_id = customer_id;
		this.stock_id = stock_id;
		this.hold_asset_avg = hold_asset_avg;
		this.hold_days = hold_days;
		this.match_cnt = match_cnt;
		this.in_match_lastdate = in_match_lastdate;
		this.hold_asset = hold_asset;
		this.total_match_cnt = total_match_cnt;
		this.total_hold_days = total_hold_days;
	}
}

