package cn.com.pander.test;

public class Test {
	public static void main(String[] args) {
		B b=new B();
		A a=new B();
		System.out.println(a.getClass().isInstance(new B()));
	}

}
class A
{
}
class B extends A
{
	}
