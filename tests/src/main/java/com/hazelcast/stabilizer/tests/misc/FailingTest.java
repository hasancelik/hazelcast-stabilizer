package com.hazelcast.stabilizer.tests.misc;

import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.utils.ExceptionReporter;

import java.util.LinkedList;
import java.util.List;

//A test that causes a failure. This is useful for testing the stabilizer framework and for demonstration purposes.
public class FailingTest {

    private TestContext testContext;

    //properties
    public String failure = "Exception";

    @Setup
    public void setup(TestContext testContext) {
        this.testContext = testContext;
    }

    @Run
    public void run() {
        if (failure.equals("Exception")) {
            ExceptionReporter.report(testContext.getTestId(), new RuntimeException("Wanted exception"));
        } else if (failure.equals("OOME")) {
            List<byte[]> list = new LinkedList<byte[]>();
            for (; ; ) {
                try {
                    list.add(new byte[100 * 1000 * 1000]);
                } catch (OutOfMemoryError error) {
                }
            }
        } else if (failure.equals("Exit")) {
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Throwable {
        FailingTest test = new FailingTest();
        new TestRunner<FailingTest>(test).run();
    }
}
