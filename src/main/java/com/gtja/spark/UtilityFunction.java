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
 *	创建时间: 2017年12月21日 - 上午10:57:16
 */
package com.gtja.spark;

import java.text.ParseException;
import java.util.Date;

/**
 * <p>
 * 定义了程序中用到的公共方法。
 * </p>
 *
 * @author lipeipei
 *
 * @version 1.0.0
 *
 * @since 1.0.0
 *
 */
public class UtilityFunction {

	/*
	 * 计算日期currentDate和lastbuyDate之间的天数。
	 */
	public static int  longOfTwoDate(Date lastbuyDate,Date currentDate) throws ParseException{    
        int days = (int) ((currentDate.getTime() - lastbuyDate.getTime()) / (24*3600*1000));  
        return days;
	}
}
