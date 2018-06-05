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

/**
 * zookeeper分布式锁初步实现：分为独占锁和共享锁
 * 独占锁实现思路：在独占锁目录下创建临时节点lock，能创建成功则获得锁，创建失败则轮询等待，监听等待别的虚拟机释放临时节点
 * 共享锁实现思路：在共享锁目录下创建临时的有序节点，这一步没有限制，都会成功，然后将共享锁目录下的子节点按顺序放置到集合中，判断
 * 第一个节点如果就是当前节点，那么直接获取锁。如果第一个节点是写锁的话，那就直接等待，然后添加一个监控。继续判断其他节点，如果
 * 判断前面有写锁的话，还是会添加一个监听，并且等待获得锁。
 * @author fly
 *
 */
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
				log.info("获得锁失败,等待ing");
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
	 * 共享锁创建临时目录
	 * @param type
	 * @param identity:身份
	 * @return
	 */
	public static boolean getSharedLock(int type,String identity) 
	{
		if(identity == null || "".equals(identity))
			throw new RuntimeException("identity is null");
		if(identity.indexOf("-")!=-1)
			throw new RuntimeException("identity 不能包含 - ");
		if(type != 0 && type!=1)
			throw new RuntimeException("type 只能为0 或者 1");
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
			log.info("创建节点："+selfNodeName);
			List<String> lockChildren=client.getChildren().forPath("/SharedLock");
			//判断是否能得到锁，不能则等待
			if(!canGetLock(lockChildren, type, nodeName.substring(0, nodeName.length()-1), false))
				shared.await();
		}catch(Exception e) 
		{
			e.printStackTrace();
			return false;
		}
		log.info("获得锁");
		return true;
	}
	
	/**
	 * 判断是否能得到锁
	 * @param children
	 * @param type
	 * @param identity：R+ip
	 * @param reps
	 * @return
	 */
	private static boolean canGetLock(List<String> children,int type,String identity,boolean reps) 
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
				//存放seq
				seqs.add(splits[1]);
				//存放seq，R+ip
				seq_identitys.put(splits[1], splits[0]);
				if(identity.equals(splits[0]))
					currentSeq=splits[1];
			}
			List<String> sortSeqs=new ArrayList<>();
			sortSeqs.addAll(seqs);
			Collections.sort(sortSeqs);
			//判断第一个节点就是自己新创建的这个节点
			if(currentSeq.equals(sortSeqs.get(0))) 
			{
				res =true;
				return res;
			}
			else 
			{
				//如果不是第一个，那么写锁一定会失败，在共享锁上加个监控
				if(type==1) 
				{
					res =false;
					if(reps==false)
						addChildWatcher("/SharedLock");
					return res;
				}
			}
			//判断读锁前面是否有写锁的存在
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
			//前面有写锁也会加一个监听，等待可以执行的那一天....
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
                		log.debug("释放独占锁");
                		exclusive.countDown();
                	}else if(path.contains("/SharedLock")) 
                	{
                		log.debug("共享锁节点"+selfIdentity+"被移除");
                		if(path.contains(selfIdentity))
                			return;
                		List<String> lockChildren=client.getChildren().forPath("/SharedLock");
                		boolean isLock =false;
                		if(selfIdentity.startsWith("R"))
                			isLock=canGetLock(lockChildren, 0, selfIdentity.substring(0, selfIdentity.length()-1), true);
                		else if(selfIdentity.startsWith("W"))
                			isLock=canGetLock(lockChildren, 1, selfIdentity.substring(0,selfIdentity.length()-1), true);
                		log.info("是否释放锁"+isLock);
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
