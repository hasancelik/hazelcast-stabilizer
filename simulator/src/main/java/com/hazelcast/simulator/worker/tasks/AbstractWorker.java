package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;

import java.util.Random;

/**
 * Abstract worker class which is returned by {@link com.hazelcast.simulator.test.annotations.RunWithWorker} annotated test
 * methods.
 *
 * Implicitly logs and measures performance. The related properties can be overwritten with the properties of the test.
 * The Operation counter is automatically increased after each {@link #timeStep(Enum)} call.
 *
 * @param <O> Type of Enum used by the {@link com.hazelcast.simulator.worker.selector.OperationSelector}
 */
public abstract class AbstractWorker<O extends Enum<O>> implements IWorker {

    static final ILogger LOGGER = Logger.getLogger(AbstractWorker.class);

    final Random random = new Random();
    final OperationSelector<O> selector;

    // these fields will be injected by the TestContainer
    TestContext testContext;
    IntervalProbe intervalProbe;

    // these fields will be injected by test.properties of the test
    long logFrequency = 10000;

    // local variables
    long iteration;

    public AbstractWorker(OperationSelectorBuilder<O> operationSelectorBuilder) {
        this.selector = operationSelectorBuilder.build();
    }

    /**
     * This constructor is just for child classes who also override the {@link #run()} method.
     */
    AbstractWorker() {
        this.selector = null;
    }

    @Override
    public void run() {
        beforeRun();

        while (!testContext.isStopped()) {
            intervalProbe.started();
            timeStep(selector.select());
            intervalProbe.done();

//            increaseIteration();
        }

        afterRun();
    }

    @Performance
    public long getOperationCount() {
        return intervalProbe.getInvocationCount();
    }

    /**
     * Override this method if you need to execute code on each worker before {@link #run()} is called.
     */
    protected void beforeRun() {
    }

    /**
     * This method is called for each iteration of {@link #run()}.
     *
     * Won't be called if an error occurs in {@link #beforeRun()}.
     *
     * @param operation The selected operation for this iteration
     */
    protected abstract void timeStep(O operation);

    /**
     * Override this method if you need to execute code on each worker after {@link #run()} is called.
     *
     * Won't be called if an error occurs in {@link #beforeRun()} or {@link #timeStep(Enum)}.
     */
    protected void afterRun() {
    }

    /**
     * Override this method if you need to execute code once after all workers have finished their run phase.
     *
     * @see IWorker
     */
    public void afterCompletion() {
    }

    /**
     * Calls {@link Random#nextInt()} on an internal Random instance.
     *
     * @return the next pseudo random, uniformly distributed {@code int} value from this random number generator's sequence
     */
    protected int randomInt() {
        return random.nextInt();
    }

    /**
     * Calls {@link Random#nextInt(int)} on an internal Random instance.
     *
     * @return the next pseudo random, uniformly distributed {@code int} value between {@code 0} (inclusive) and {@code n}
     * (exclusive) from this random number generator's sequence
     */
    protected int randomInt(int upperBond) {
        return random.nextInt(upperBond);
    }

    /**
     * Returns the inner {@link Random} instance to call methods which are not implemented.
     *
     * @return the {@link Random} instance of the worker
     */
    protected Random getRandom() {
        return random;
    }

    void increaseIteration() {
        iteration++;
        if (iteration % logFrequency == 0) {
            LOGGER.info(Thread.currentThread().getName() + " At iteration: " + iteration);
        }
    }
}
