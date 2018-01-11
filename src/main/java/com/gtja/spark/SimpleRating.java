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
 *	创建时间: 2018年1月10日 - 下午3:37:57
 */
package com.gtja.spark;

import java.io.Serializable;

/**
 * <p>
 * @TODO 请“lipeipei” 尽快添加代码注释!（中文表达，简要说明）
 * </p>
 *
 * @author lipeipei
 *
 * @version 1.0.0
 *
 * @since 1.0.0
 *
 */
public class SimpleRating implements Serializable {

	private String customer_no;
	private String stock_code;
	private double score;
	/**
	 * @return the customer_no
	 */
	public String getCustomer_no() {
		return customer_no;
	}
	/**
	 * @param customer_no the customer_no to set
	 */
	public void setCustomer_no(String customer_no) {
		this.customer_no = customer_no;
	}
	/**
	 * @return the stock_code
	 */
	public String getStock_code() {
		return stock_code;
	}
	/**
	 * @param stock_code the stock_code to set
	 */
	public void setStock_code(String stock_code) {
		this.stock_code = stock_code;
	}
	/**
	 * @return the score
	 */
	public double getScore() {
		return score;
	}
	/**
	 * @param score the score to set
	 */
	public void setScore(double score) {
		this.score = score;
	}
	/**
	 * @param customer_no
	 * @param stock_code
	 * @param score
	 */
	public SimpleRating(String customer_no, String stock_code, double score) {
		super();
		this.customer_no = customer_no;
		this.stock_code = stock_code;
		this.score = score;
	}
	
	
}
