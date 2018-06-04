package cn.com.pander.zookeeper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;


import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

public class ZookeeperLock {
	private static CuratorFramework client = ZookeeperClient.getInsatnce();
	private static Logger log = Logger.getLogger(ZookeeperLock.class);
	protected static CountDownLatch exclusive =new CountDownLatch(1);
	protected static CountDownLatch shared = new CountDownLatch(1);
	private static String selfIdentity=null;
	private static String selfNodeName=null;
	
	/**
	 * 初始化节点，并在独占锁绑定监听
	 * @throws Exception
	 */
	public static synchronized void init() throws Exception 
	{
		if(client.checkExists().forPath("/ExclusiveLock") == null) 
		{
			client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
			.forPath("/ExclusiveLock");
		}
		addChildWatcher("/ExclusiveLock");
		if(client.checkExists().forPath("/SharedLock") == null) 
		{
			client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
			.forPath("/SharedLock");
		}
	}
	
	/**
	 * 尝试获取独占锁
	 */
	public static synchronized void getExclusiveLock() 
	{
		while(true) 
		{
			try {
				client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
				.forPath("/ExclusiveLock/lock");
				log.info("获得锁成功");
				return ;
			} catch (Exception e) {
				e.printStackTrace();
				log.info("获得锁失败");
				try {
					if(exclusive.getCount()<=0) 
						exclusive=new CountDownLatch(1);
					exclusive.await();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
		
	}
	
	/**
	 * 尝试获得共享锁
	 * @param type
	 * @param identity
	 * @return
	 */
	public static boolean getSharedLock(int type,String identity) 
	{
		if(identity == null || "".equals(identity))
			throw new RuntimeException("identity is null");
		if(identity.indexOf("-")!=-1)
			throw new RuntimeException();
		if(type != 0 && type!=1)
			throw new RuntimeException();
		String nodeName =null;
		if(type ==0)
			nodeName="R"+identity+"-";
		else if(type == 1)
			nodeName="W"+identity+"-";
		selfIdentity =nodeName;
		try 
		{
			selfNodeName=client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
					.forPath("/SharedLock/"+nodeName);
		}catch(Exception e) 
		{
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * 对共享锁的操作
	 * @param children
	 * @param type
	 * @param identity
	 * @param reps
	 * @return
	 */
	private static boolean getLock(List<String> children,int type,String identity,boolean reps) 
	{
		boolean res =false;
		if(children.size()<=0)
			return true;
		try 
		{
			String currentSeq=null;
			List<String> seqs=new ArrayList<>();
			Map<String, String> seq_identitys=new HashMap<>();
			for(String child : children) 
			{
				String[] splits=child.split("-");
				seqs.add(splits[1]);
				seq_identitys.put(splits[1], splits[0]);
				if(identity.equals(splits[0]))
					currentSeq=splits[1];
			}
			List<String> sortSeqs=new ArrayList<>();
			sortSeqs.addAll(seqs);
			Collections.sort(sortSeqs);
			if(currentSeq.equals(sortSeqs.get(0))) 
			{
				res =true;
				return res;
			}
			else 
			{
				if(type==1) 
				{
					res =false;
					if(reps==false)
						addChildWatcher("/SharedLock");
					return res;
				}
			}
			boolean hasw=true;
			for(String seq : sortSeqs) 
			{
				if(seq.equals(currentSeq))
					break;
				if(!seq_identitys.get(seq).startsWith("W"))
					hasw=false;
			}
			if(type == 0 && hasw ==false)
				res =true;
			else if(type==0&&hasw==true)
				res=false;
			if(res==false)
				addChildWatcher("/SharedLock");
		}catch(Exception e) 
		{
			e.printStackTrace();
		}
		return res;
	}
	
	/**
	 * 对锁节点的监听
	 * @param path
	 * @throws Exception
	 */
	public static void addChildWatcher(String path) throws Exception 
	{
		final PathChildrenCache cache = new PathChildrenCache(client, path, true);
		cache.start(StartMode.POST_INITIALIZED_EVENT);
		cache.getListenable().addListener(new PathChildrenCacheListener() {
			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                if(event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) 
                    log.debug("PathChildrenCache初始化");
                else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED))
                	log.debug(event.getData().getPath()+"子节点添加");
                else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) 
                {
                	String path =event.getData().getPath();
                	System.out.println("子节点 : "+path+" 被移除");
                	if(path.contains("/ExclusiveLock")) 
                	{
                		log.debug("独占锁");
                		exclusive.countDown();
                	}else if(path.contains("/SharedLock")) 
                	{
                		log.debug("共享锁");
                		if(path.contains(selfIdentity))
                			return;
                		List<String> lockChildren=client.getChildren().forPath("/SharedLock");
                		boolean isLock =false;
                		if(selfIdentity.startsWith("R"))
                			isLock=getLock(lockChildren, 0, selfIdentity.substring(0, selfIdentity.length()-1), true);
                		else if(selfIdentity.startsWith("W"))
                			isLock=getLock(lockChildren, 1, selfIdentity.substring(0,selfIdentity.length()-1), true);
                		log.info("是否获得锁"+isLock);
                		if(isLock)
                			shared.countDown();
                	}
                }else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED))
                	System.out.println("child数据节点更新");
			}
		});
	}
	
	
	public static boolean unlockForExclusive()
	{
		try {
			if(client.checkExists().forPath("/ExclusiveLock/lock")!=null)
				client.delete().forPath("/ExclusiveLock/lock");
		} catch (Exception e) {
			return false;
		}
		return true;
	}
	
	public static boolean unlockForSharedLock()
	{
			try {
				if(client.checkExists().forPath(selfNodeName)!=null)
				client.delete().forPath(selfNodeName);
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		return true;
	}

}
