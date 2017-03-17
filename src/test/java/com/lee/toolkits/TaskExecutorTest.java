package com.lee.toolkits;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TaskExecutorTest {

    private static TaskExecutor executor;
    private static int[] elements;

    @BeforeClass
    public static void init() throws Exception {
        executor = new TaskExecutor("taskExecutorTest", 0L, TimeUnit.SECONDS);
        Random random = new Random();
        elements = new int[100];
        for(int i=0; i<100; i++) { elements[i] = random.nextInt(); }
    }

    @AfterClass
    public static void destroy() throws Exception {
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    // @Test
    public void testSingleResult() throws Exception {
        Long expect = sum(elements, 0, elements.length);
        List<TaskExecutor.Computable<Long>> tasks = new ArrayList<TaskExecutor.Computable<Long>>(5);
        tasks.add(newTask("SingleSumTask-1", 0, elements.length));
        tasks.add(newTask("SingleSumTask-2", 0, elements.length));
        tasks.add(newTask("SingleSumTask-3", 0, elements.length));
        tasks.add(newTask("SingleSumTask-4", 0, elements.length));
        tasks.add(newTask("SingleSumTask-5", 0, elements.length));
        TaskExecutor.SingleResult<Long> result = executor.invokeAny(tasks);
        Long sum = result.get(5L, TimeUnit.SECONDS);
        Assert.assertEquals(expect, sum);
        TimeUnit.SECONDS.sleep(5);  // wait for cat sync
    }

    // @Test
    public void testMultiResult() throws Exception {
        int range = elements.length / 5;
        Long[] expects = new Long[5];
        for(int i=0; i<5; i++) { expects[i] = sum(elements, i*range, (i+1)*range); }
        List<TaskExecutor.Computable<Long>> tasks = new ArrayList<TaskExecutor.Computable<Long>>(5);
        tasks.add(newTask("MultiSumTask-1", 0, range));
        tasks.add(newTask("MultiSumTask-2", range, range*2));
        tasks.add(newTask("MultiSumTask-3", range*2, range*3));
        tasks.add(newTask("MultiSumTask-4", range*3, range*4));
        tasks.add(newTask("MultiSumTask-5", range*4, range*5));
        TaskExecutor.MultiResult<Long> result = executor.invokeMulti(tasks);
        for(int i=0; i<5; i++) {
            Long sum = result.get(i, 5L, TimeUnit.SECONDS);
            Assert.assertEquals(expects[i], sum);
        }
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    public void testTripleResult() throws Exception {
        Long expect1 = sum(elements, 0, 20);
        BigInteger expect2 = BigInteger.valueOf(sum(elements, 21, 50));
        BigDecimal expect3 = BigDecimal.valueOf(sum(elements, 51, elements.length));
        TaskExecutor.Computable<Long> task1 = newTaskWithType("TripleSumTask-1", 0, 20, Long.class);
        TaskExecutor.Computable<BigInteger> task2 = newTaskWithType("TripleSumTask-2", 21, 50, BigInteger.class);
        TaskExecutor.Computable<BigDecimal> task3 = newTaskWithType("TripleSumTask-3", 51, elements.length, BigDecimal.class);
        TaskExecutor.TripleResult<Long, BigInteger, BigDecimal> result = executor.invokeAll(task1, task2, task3);
        Long sum1 = result.getFirstResult(5L, TimeUnit.SECONDS);
        Assert.assertEquals(expect1, sum1);
        BigInteger sum2 = result.getSecondResult(5L, TimeUnit.SECONDS);
        Assert.assertEquals(expect2, sum2);
        BigDecimal sum3 = result.getThirdResult(5L, TimeUnit.SECONDS);
        Assert.assertEquals(expect3, sum3);
        TimeUnit.SECONDS.sleep(5);
    }

    // @Test
    public void testTupleResult() throws Exception {
        int range = elements.length / 10;
        long[] expects = new long[10];
        for(int i=0; i<10; i++) { expects[i] = sum(elements, i*range, (i+1)*range); }
        List<TaskExecutor.Computable> tasks = new ArrayList<TaskExecutor.Computable>(10);
        for(int i=0; i<10; i++) {
            int k = i % 3;
            int start = i * range, end = start + range;
            if(k == 0) {
                tasks.add(newTaskWithType("TupleSumTask-" + (i + 1), start, end, Long.class));
            }else if(k == 1) {
                tasks.add(newTaskWithType("TupleSumTask-" + (i + 1), start, end, BigInteger.class));
            }else {
                tasks.add(newTaskWithType("TupleSumTask-" + (i + 1), start, end, BigDecimal.class));
            }
        }
        TaskExecutor.TupleResult result = executor.invokeAll(tasks);
        for(int i=0; i<10; i++) {
            int k = i % 3;
            if(k == 0) {
                Long sum = result.get(i, 5L, TimeUnit.SECONDS);
                Assert.assertEquals(expects[i], sum.longValue());
            }else if(k == 1) {
                BigInteger sum = result.get(i, 5L, TimeUnit.SECONDS);
                Assert.assertEquals(expects[i], sum.longValue());
            }else {
                BigDecimal sum = result.get(i, 5L, TimeUnit.SECONDS);
                Assert.assertEquals(expects[i], sum.longValue());
            }
        }
        TimeUnit.SECONDS.sleep(5);
    }

    private static TaskExecutor.Computable<Long> newTask(final String taskName, final int start, final int end) {
        return new TaskExecutor.Computable<Long>(taskName) {
            @Override
            public Long compute() throws Exception {
                int sleepTime = new Random().nextInt(100);
                long s = sum(elements, start, end, sleepTime);
                return s;
            }
        };
    }

    private static <T extends Number> TaskExecutor.Computable<T> newTaskWithType(final String taskName,
            final int start, final int end, final Class<T> clazz) {
        return new TaskExecutor.Computable<T>(taskName) {
            @Override
            public T compute() throws Exception {
                int sleepTime = new Random().nextInt(100);
                long s = sum(elements, start, end, sleepTime);
                if(clazz == Long.class) {
                    return (T) Long.valueOf(s);
                }else if(clazz == BigInteger.class) {
                    return (T) BigInteger.valueOf(s);
                }else if(clazz == BigDecimal.class) {
                    return (T) BigDecimal.valueOf(s);
                }else {
                    throw new ClassCastException("can not cast long value to "+clazz);
                }
            }
        };
    }

    private static long sum(int[] elements, int start, int end, int sleepTime) throws Exception {
        long result = sum(elements, start, end);
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        return result;
    }

    private static long sum(int[] elements, int start, int end) {
        long result = 0;
        for(int i=start; i<end; i++) {
            result += elements[i];
        }
        return result;
    }
}
