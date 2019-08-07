import org.junit.Test;

public class test {
    @Test
    public void test(){
        for (int i=1;i<10;i++) {
            System.out.println(Math.random());
        }
    }
    @Test
    public void test1() throws InterruptedException {
        for (int i=1;i<20;i++){
            System.out.println(System.currentTimeMillis());
            Thread.sleep(50);
        }
    }
}
