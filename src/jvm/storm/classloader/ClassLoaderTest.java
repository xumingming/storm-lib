package storm.classloader;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Test Result:
 * <code>
 * warm up [testChangeClassLoader] : 7287000 nano-seconds
 * warm up [testAssign           ] : 182000 nano-seconds
 * real test [testChangeClassLoader] : 66233000 nano-seconds
 * real test [testAssign           ] : 10928000 nano-seconds
 * </code>
 * @author xumingmingv Jun 14, 2012 8:50:39 PM
 */
public class ClassLoaderTest {

    public static void main(String[] args) throws MalformedURLException {
        String jarfilePath = "lib/clojure-1.3.0.jar";
        URLClassLoader loader = new URLClassLoader(new URL[]{new File(jarfilePath).toURI().toURL()});
        int target = 0;
        
        // warm up
        testChangeClassLoader(loader, "warm up [testChangeClassLoader]", 10000);
        testAssign(target,            "warm up [testAssign           ]", 10000);
        
        // run-test
        testChangeClassLoader(loader, "real test [testChangeClassLoader]", 10000000);
        testAssign(target,            "real test [testAssign           ]", 10000000);
            
        // reference loader and target here to make sure JVM does not wrongly optimize
        System.out.println(loader);
        System.out.println(target);
    }
    
    public static void testChangeClassLoader(ClassLoader loader, String message, int times) {
        long start = System.nanoTime();
        for (int i = 0; i < times; i++) {
            Thread.currentThread().setContextClassLoader(loader);
        }
        long end = System.nanoTime();
        
        System.out.println(message + " : " + (end - start) + " nano-seconds");
    }
    
    public static void testAssign(int target, String message, int times) {
        long start = System.nanoTime();
        for (int i = 0; i < times; i++) {
            target = i;
        }
        long end = System.nanoTime();
        
        System.out.println(message + " : " + (end - start) + " nano-seconds");
    }
}
