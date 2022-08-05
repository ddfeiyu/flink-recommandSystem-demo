package com.demo.scheduler;

import com.demo.client.HbaseClient;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * FIXME :离线引擎 每12小时定时调度一次 基于两个推荐策略的 产品评分计算
 * FIXME :策略1 ：协同过滤
 *        数据写入Hbase表  px
 *
 * FIXME :策略2 ： 基于产品标签 计算产品的余弦相似度
 *        数据写入Hbase表 ps
 *
 */
public class SchedulerJob {

	static ExecutorService executorService = Executors.newFixedThreadPool(10);

	/**
	 * 每12小时定时调度一次 基于两个推荐策略的 产品评分计算
	 * 策略1 ：协同过滤
	 *
	 *        数据写入Hbase表  px
	 *
	 * 策略2 ： 基于产品标签 计算产品的余弦相似度
	 *
	 *        数据写入Hbase表 ps
	 *
	 * @param args
	 */
	public static void main(String[] args) {
		//		ScheduledExecutorService pool = new ScheduledThreadPoolExecutor(5);
		Timer qTimer = new Timer();
		qTimer.scheduleAtFixedRate(new RefreshTask(), 0, 15 * 1000);// 定时每15分钟


	}

	private static class RefreshTask extends TimerTask {



		@Override
		public void run() {
			System.out.println(new Date() + " 开始执行任务！");
			/**
			 * 取出被用户点击过的产品id，getAllKey只是一个示例，真实情况不太可能把所有的产品取出来
			 *
			 */
			List<String> allProId = new ArrayList<>();
			try {
				allProId = HbaseClient.getAllKey("p_history");
			} catch (IOException e) {
				System.err.println("获取历史产品id异常: " + e.getMessage());
				e.printStackTrace();
				return;
			}
			/**
			 * 可以考虑任务执行前是否需要把历史记录删掉
			 */
			for (String id : allProId) {
				// 每12小时调度一次
				executorService.execute(new Task(id, allProId));
			}
		}
	}

	private static class Task implements Runnable {

		private String id;
		private List<String> others;

		public Task(String id, List<String> others) {
			this.id = id;
			this.others = others;
		}


		ItemCfCoeff item = new ItemCfCoeff();
		ProductCoeff prod = new ProductCoeff();

		@Override
		public void run() {
			try {
				/**
				 * FIXME  基于协同过滤的产品相关度计算 , 存入 Hbase表 px
				 *  根据p_history（产品-用户关联表），计算两个产品之间的评分
				 */
				item.getSingelItemCfCoeff(id, others);
				/**
				 * FIXME  基于产品标签的产品相关度计算
				 *  根据标签计算两个产品之间的相关度, 计算一个产品和其他相关产品的评分,并将计算结果放入Hbase , 存入 Hbase表 ps
				 */
				prod.getSingelProductCoeff(id, others);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}


}
