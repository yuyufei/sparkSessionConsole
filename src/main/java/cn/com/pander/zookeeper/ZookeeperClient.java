package cn.com.pander.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZookeeperClient {
	
	private static CuratorFramework client =null;
	
	public static CuratorFramework getInsatnce() 
	{
		if(null  == client) 
		{
			synchronized (ZookeeperClient.class) {
				if(null == client) 
				{
					RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000, 3);
					client= CuratorFrameworkFactory.builder()
							.connectString("master:2181,slave1:2181,slave2:2181")
				            .sessionTimeoutMs(10000).retryPolicy(retryPolicy)
				            .namespace("Lock").build();
					client.start();
					System.out.println(client.toString());
					return client;
				}
			}
		}
		return client;
	}

}
