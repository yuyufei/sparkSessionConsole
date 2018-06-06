package cn.com.pander.test;

/**
 * 突然想到synchronized的一种情况，如果是父类中有加锁的方法，但是锁的实例不是同一个，锁就是无效的，必须有一个都可以确认的。
 * @author fly
 *
 */
public class SyncornizedTest {
	public static void main(String args[]) {
		InnerRun mt1 = new InnerRun(new ManService(),"man");
		InnerRun mt2 = new InnerRun(new ManService(),"man");
	    Thread t1 = new Thread(mt1, "t1");
	    Thread t2 = new Thread(mt2, "t2");
	    t1.start();
	    t2.start();
		
	  }


}

class InnerRun implements Runnable
{

	private AbstractPersonService o;
	private String name;
	
	public InnerRun(AbstractPersonService o,String name) 
	{
		this.o=o;
		this.name=name;
	}
	
	@Override
	public void run() {
		try {
			o.test1();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
          System.out.println(name+"--------");		
	}
	
}
