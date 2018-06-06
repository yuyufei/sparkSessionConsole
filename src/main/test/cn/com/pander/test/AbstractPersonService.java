package cn.com.pander.test;

public abstract class AbstractPersonService {
	private  Object keyLock =new Object();
	private Object valueLock =new Object();
	
	public final void test1() throws InterruptedException 
	{
//		synchronized(this) 锁是无效的
//		synchronized(AbstractPersonService.class) 这个是对多实例有效，锁的是类对象
		/**
		 * 无意间看到一个同步框调用同步框的代码，AbstractSelectableChannel的register方法，一开始觉得就需要一个同步块就可以，
		 * 是自己太low了，同步块只能保证基础操作的原子性，但是如果在同步块调用方法去设置一个公共变量，那么就是不同步的，因为
		 * 其他线程同样可以调用同步块里面使用的方法，或者通过其他途径也可以修改这个公共变量，所以修改这个公共变量的时候，也需要
		 * 对这个公共变量给它加一把obj锁来保证操作它的原子性。
		 */
		synchronized(keyLock) 
		{
			  System.out.println("keylock");
			  Thread.sleep(2000);
			  System.out.println("over");
			  
			  synchronized (valueLock) {
			System.out.println("valuelock");	
			}
		}
	}

	public abstract void showSex() ;
}
