package cn.com.pander.test;

import java.util.concurrent.CountDownLatch;

import cn.com.pander.zookeeper.ZookeeperClient;

public class Test {
	public static void main(String[] args) throws InterruptedException {
		CountDownLatch latch=new CountDownLatch(100);
		for(int i=0;i<100;i++) 
		{
			Thread t=new Thread(new Runnable() {
				@Override
				public void run() {
						latch.countDown();
                  System.out.println(ZookeeperClient.getInsatnce());					
				}
			});
			t.start();
		}
		System.out.println("kaishidengdai ");
		latch.await();
	}

}
