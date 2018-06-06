package cn.com.pander.test;


public class LiuRongshuiIdea {
	public static void main(String[] args) {
		Abss i=new Abss();
		i.test(i);
	}

}

class Abss
{
	private Object obj1=new Object();
	private Object obj2=new Object();
	public void test(Abss a) 
	{
		synchronized (obj1) {
			System.out.println("obj1");
			Thread t1=new Thread(new Runnable() {
				
				@Override
				public void run() {
                    					a.test(a);
				}
			});
			Thread t2=new Thread(new Runnable() {
				
				@Override
				public void run() {
					a.test(a);
					
				}
			});
			t1.start();
			t2.start();
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			synchronized (obj2) {
				System.out.println(obj2);
			}
		}
	}
}
