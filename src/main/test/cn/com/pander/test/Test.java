package cn.com.pander.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import cn.com.pander.zookeeper.ZookeeperClient;

public class Test {
	
	public void init() throws IOException 
	{
		ServerSocketChannel channel=ServerSocketChannel.open();
		Selector selector=SelectorProvider.provider().openSelector();
		InetSocketAddress address=new InetSocketAddress(8090);
		channel.socket().bind(address);
		channel.register(selector,SelectionKey.OP_ACCEPT );
		while(true) 
		{
			int n=selector.select();
			if(n>0) 
			{
				Iterator<SelectionKey>ite= selector.selectedKeys().iterator();
				while(ite.hasNext()) 
				{
					SelectionKey k=ite.next();
//					k.channel().
					ite.remove();
				}
			}
		}
	}
	
	
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
