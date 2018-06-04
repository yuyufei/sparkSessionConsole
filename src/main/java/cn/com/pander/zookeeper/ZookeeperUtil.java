package cn.com.pander.zookeeper;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

public class ZookeeperUtil {
	
	private CuratorFramework client = ZookeeperClient.getInsatnce();
	
	public void createNode(String path,byte[] data) throws Exception 
	{
//		client.getZookeeperClient().getZooKeeper().addAuthInfo("digest", "abcd:111111".getBytes());
	    String result=client.create().creatingParentsIfNeeded()
	    .withMode(CreateMode.PERSISTENT).forPath(path);
	    System.out.println(result);
	}
	
	public void deleteNode(String path,int version) throws Exception 
	{
		client.delete().guaranteed().deletingChildrenIfNeeded().withVersion(version)
		.inBackground(new BackGroundCallBackImpl()).forPath(path);
	}
	
	public void readNode(String path) throws Exception 
	{
		Stat stat=new Stat();
		byte[] data=client.getData().storingStatIn(stat).forPath(path);
		System.out.println("读取节点" + path + "的数据:" + new String(data));
		System.out.println(stat.toString());
	}
	
	public void updateNode(String path,byte[] data,int version) throws Exception 
	{
		client.setData().inBackground(new BackGroundCallBackImpl()).forPath(path,data);
	}
	
	public void getChildren(String path) throws Exception 
	{
		List<String> children = client.getChildren().usingWatcher(new InnerWatcher())
				.forPath(path);
		children.forEach(child -> {System.out.println(path+"子节点有 : "+child);});
	}
	
	/**
	 * node节点变化监听
	 * @param path
	 * @throws Exception
	 */
	public void addNodeDataWacher(String path) throws Exception 
	{
		final NodeCache nodeCache=new NodeCache(client, path);
		nodeCache.start(true);
		nodeCache.getListenable().addListener(new NodeCacheListener() {
			@Override
			public void nodeChanged() throws Exception {
                System.out.println("path = "+nodeCache.getCurrentData().getPath()
                		+", data = "+nodeCache.getCurrentData().getData().toString());				
			}
		});
	}
	
	/**
	 * 监听子节点的变化
	 * @param path
	 * @throws Exception
	 */
	public void addChildWatcher(String path) throws Exception 
	{
		final PathChildrenCache cache=new PathChildrenCache(this.client, path, true);
		cache.start(StartMode.POST_INITIALIZED_EVENT);
		System.out.println("PathChildrenCache's size : "+cache.getCurrentData().size());
		cache.getListenable().addListener(new PathChildrenCacheListener() {
			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                if(event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) 
                {
                	System.out.println("客户端子节点cache初始化数据完成");
                	System.out.println("size = "+cache.getCurrentData().size());
                }else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)){
					System.out.println("添加子节点:"+event.getData().getPath());
					System.out.println("修改子节点数据:"+new String(event.getData().getData()));
				}else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)){
					System.out.println("删除子节点:"+event.getData().getPath());
				}else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)){
					System.out.println("修改子节点数据:"+event.getData().getPath());
					System.out.println("修改子节点数据:"+new String(event.getData().getData()));
				}				
			}
		});
	}
	
	public static void main(String[] args) throws Exception {
		ZookeeperUtil util=new ZookeeperUtil();
//		util.createNode("/curator", "test".getBytes());
//		util.deleteNode("/curator", 0);
//		util.readNode("/curator");
//		util.updateNode("/curator", "gg".getBytes(), 1);
//		util.getChildren("/curator");
//		util.addNodeDataWacher("/curator");
		util.addChildWatcher("/curator");
		Thread.sleep(300000);
//		util.client.close();
	}

}

class BackGroundCallBackImpl implements BackgroundCallback
{

	@Override
	public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
        System.out.println("eventpath : "+event.getPath()+", data = "+event.getData());		
	}
	
}

class InnerWatcher implements CuratorWatcher
{

	@Override
	public void process(WatchedEvent event) throws Exception {
        System.out.println(event.toString());		
	}
}