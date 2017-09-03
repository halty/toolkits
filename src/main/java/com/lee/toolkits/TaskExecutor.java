package com.lee.toolkits;

import java.util.ArrayList;
import java.util.List;
import java.util.RandomAccess;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class TaskExecutor {

    public static final int DEFAULT_WAIT_QUEUE_LENGTH = 500;
    
    static final TransactionFactory TRANSACTION_FACTORY = loadTransactionFactory();

    private final String taskType;
    private final AtomicInteger threadNameSeq;
    private final ExecutorService executorService;
    
    private static TransactionFactory loadTransactionFactory() {
    	ServiceLoader<TransactionFactory> loader = ServiceLoader.load(TransactionFactory.class);
        for(TransactionFactory factory : loader) {
        	if(factory != null) {
        		return factory;
        	}
        }
        throw new IllegalStateException("no transaction factory found");
    }

    /**
     * creates a task executor with default parallelism {@link java.lang.Runtime#availableProcessors}
     * and default waitQueueLength {@link #DEFAULT_WAIT_QUEUE_LENGTH}.
     */
    public TaskExecutor(final String taskType,
                        long idleTime, TimeUnit unit) {
        this(taskType, Runtime.getRuntime().availableProcessors(), DEFAULT_WAIT_QUEUE_LENGTH, idleTime, unit);
    }

    /**
     * creates a task executor with given init parameters
     * @param taskType  type of execution tasks, can not be <code>null</code>
     * @param parallelism   the number of threads to keep in the executor, even if they are idle, need > 0
     * @param waitQueueLength   the length of waiting queue which holding tasks before they are executed, need > 0
     * @param idleTime  when the number of threads exceed <code>parallelism</code>, this is the maximum time that
     *                  excess idle threads will wait for new tasks before terminating, need >= 0
     * @param unit  the time unit for the <code>idleTime</code>, can not be <code>null</code>
     */
    public TaskExecutor(final String taskType, int parallelism, int waitQueueLength,
                        long idleTime, TimeUnit unit) {
        if(taskType == null) {
            throw new IllegalArgumentException("task type is null");
        }
        if(parallelism <= 0) {
            throw new IllegalArgumentException("parallelism need > 0");
        }
        if(waitQueueLength <= 0) {
            throw new IllegalArgumentException("waitQueueLength need > 0");
        }
        if(idleTime < 0 || unit == null) {
            throw new IllegalArgumentException("idleTime need > 0 and unit can not be null");
        }
        this.taskType = taskType;
        this.threadNameSeq = new AtomicInteger(0);
        this.executorService = new ThreadPoolExecutor(parallelism, parallelism << 1, idleTime, unit,
            new ArrayBlockingQueue<Runnable>(waitQueueLength),
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(null, r,
                        String.format("%s-thread-%d", taskType, threadNameSeq.getAndIncrement()),
                        0);
                    if (t.isDaemon())
                        t.setDaemon(false);
                    if (t.getPriority() != Thread.NORM_PRIORITY)
                        t.setPriority(Thread.NORM_PRIORITY);
                    return t;
                }
            });
    }

    /**
     * submits the given tasks, returning the result of one that has completed successfully.
     * It works as if by invoking {@link #invokeAny(List, boolean)} with given tasks and
     * <code>fastFirst</code> argument of <code>false</code>.
     */
    public <V> SingleResult<V> invokeAny(List<Computable<V>> tasks) {
        return invokeAny(tasks, false);
    }

    /**
     * submits the given tasks, returning the result of one that has completed.
     * if <code>fastFirst</code> is true, returning the first completed result whether it is successful
     * or not, otherwise returning the result of one that has completed successfully.
     * returning exceptional result if all tasks failed.
     */
    public <V> SingleResult<V> invokeAny(List<Computable<V>> tasks, boolean fastFirst) {
        if(tasks == null || tasks.size() == 0) {
            throw new IllegalArgumentException("tasks is empty");
        }
        List<Task> taskWrappers = new ArrayList<Task>(tasks.size());
        SingleResult<V> result = new SingleResult<V>(taskType, taskWrappers, fastFirst);
        int i = 0;
        for(Computable<V> t : tasks) {
            Task wrapper = new Task(t, i++, result);
            taskWrappers.add(wrapper);
        }
        result.startSubmission();
        try {
            for (Task tw : taskWrappers) { executorService.execute(tw); }
        }finally { result.completeSubmission(); }
        return result;
    }

    /**
     * submits the given tasks, returning the result of submission.
     * specialization for {@link #invokeAll(List)} within all tasks outcome has the same type.
     */
    public <V> MultiResult<V> invokeMulti(List<Computable<V>> tasks) {
        int taskCapacity = (tasks == null ? 0 : tasks.size());
        if(taskCapacity == 0) {
            throw new IllegalArgumentException("tasks is empty");
        }
        List<Task> taskWrappers = new ArrayList<Task>(taskCapacity);
        MultiResult<V> result = new MultiResult<V>(taskType, taskWrappers, taskCapacity);
        int i = 0;
        for(Computable<V> t : tasks) {
            if(t == null) { throw new IllegalArgumentException("has null task"); }
            Task wrapper = new Task(t, i++, result);
            taskWrappers.add(wrapper);
        }
        result.startSubmission();
        try {
            for (Task tw : taskWrappers) { executorService.execute(tw); }
        }finally { result.completeSubmission(); }
        return result;
    }

    /**
     * submits the given tasks, returning the result of submission.
     * specialization for {@link #invokeAll(List)} with two tasks.
     */
    public <L, R> PairResult<L, R> invokeAll(Computable<L> firstTask, Computable<R> secondTask) {
        if(firstTask == null || secondTask == null) {
            throw new IllegalArgumentException("tasks is empty");
        }
        List<Task> taskWrappers = new ArrayList<Task>(2);
        PairResult<L, R> result = new PairResult<L, R>(taskType, taskWrappers, 2);
        taskWrappers.add(new Task(firstTask, 0, result));
        taskWrappers.add(new Task(secondTask, 1, result));
        result.startSubmission();
        try {
            for (Task tw : taskWrappers) { executorService.execute(tw); }
        }finally { result.completeSubmission(); }
        return result;
    }

    /**
     * submits the given tasks, returning the result of submission.
     * specialization for {@link #invokeAll(List)} with three tasks.
     */
    public <L, M, R> TripleResult<L, M, R> invokeAll(Computable<L> firstTask, Computable<M> secondTask, Computable<R> thirdTask) {
        if(firstTask == null || secondTask == null || thirdTask == null) {
            throw new IllegalArgumentException("has null task");
        }
        List<Task> taskWrappers = new ArrayList<Task>(3);
        TripleResult<L, M, R> result = new TripleResult<L, M, R>(taskType, taskWrappers, 3);
        taskWrappers.add(new Task(firstTask, 0, result));
        taskWrappers.add(new Task(secondTask, 1, result));
        taskWrappers.add(new Task(thirdTask, 2, result));
        result.startSubmission();
        try {
            for (Task tw : taskWrappers) { executorService.execute(tw); }
        }finally { result.completeSubmission(); }
        return result;
    }

    /**
     * submits the given tasks, returning the result of submission.
     * specialization for {@link #invokeAll(List)} with four tasks.
     */
    public <V1, V2, V3, V4> FourResult<V1, V2, V3, V4> invokeAll(Computable<V1> firstTask, Computable<V2> secondTask,
                                                                 Computable<V3> thirdTask, Computable<V4> fourthTask) {
        if(firstTask == null || secondTask == null || thirdTask == null || fourthTask == null) {
            throw new IllegalArgumentException("has null task");
        }
        List<Task> taskWrappers = new ArrayList<Task>(4);
        FourResult<V1, V2, V3, V4> result = new FourResult<V1, V2, V3, V4>(taskType, taskWrappers, 4);
        taskWrappers.add(new Task(firstTask, 0, result));
        taskWrappers.add(new Task(secondTask, 1, result));
        taskWrappers.add(new Task(thirdTask, 2, result));
        taskWrappers.add(new Task(fourthTask, 3, result));
        result.startSubmission();
        try {
            for (Task tw : taskWrappers) { executorService.execute(tw); }
        }finally { result.completeSubmission(); }
        return result;
    }

    /**
     * submits the given tasks, returning the result of submission.
     * specialization for {@link #invokeAll(List)} with five tasks.
     */
    public <V1, V2, V3, V4, V5> FiveResult<V1, V2, V3, V4, V5> invokeAll(Computable<V1> firstTask,
            Computable<V2> secondTask, Computable<V3> thirdTask, Computable<V4> fourthTask, Computable<V5> fifthTask) {
        if(firstTask == null || secondTask == null || thirdTask == null || fourthTask == null || fifthTask == null) {
            throw new IllegalArgumentException("has null task");
        }
        List<Task> taskWrappers = new ArrayList<Task>(5);
        FiveResult<V1, V2, V3, V4, V5> result = new FiveResult<V1, V2, V3, V4, V5>(taskType, taskWrappers, 5);
        taskWrappers.add(new Task(firstTask, 0, result));
        taskWrappers.add(new Task(secondTask, 1, result));
        taskWrappers.add(new Task(thirdTask, 2, result));
        taskWrappers.add(new Task(fourthTask, 3, result));
        taskWrappers.add(new Task(fifthTask, 4, result));
        result.startSubmission();
        try {
            for (Task tw : taskWrappers) { executorService.execute(tw); }
        }finally { result.completeSubmission(); }
        return result;
    }

    /** submits the given tasks, returning the result of submission **/
    public TupleResult invokeAll(List<Computable> tasks) {
        int taskCapacity = (tasks == null ? 0 : tasks.size());
        if(taskCapacity == 0) {
            throw new IllegalArgumentException("tasks is empty");
        }
        List<Task> taskWrappers = new ArrayList<Task>(taskCapacity);
        TupleResult result = new TupleResult(taskType, taskWrappers, taskCapacity);
        int i = 0;
        for(Computable t : tasks) {
            if(t == null) { throw new IllegalArgumentException("has null task"); }
            Task wrapper = new Task(t, i++, result);
            taskWrappers.add(wrapper);
        }
        result.startSubmission();
        try {
            for (Task tw : taskWrappers) { executorService.execute(tw); }
        }finally { result.completeSubmission(); }
        return result;
    }

    /**
     * Initiates an orderly shutdown in which previously submitted
     * tasks are executed, but no new tasks will be accepted.
     * Invocation has no additional effect if already shut down.
     *
     * <p>This method does not wait for previously submitted tasks to
     * complete execution. Use {@link #awaitTermination awaitTermination}
     * to do that.
     *
     * wrap for inner {@link ExecutorService}.
     */
    public void shutdown() { executorService.shutdown(); }

    /**
     * Returns <tt>true</tt> if this executor has been shut down.
     *
     * wrap for inner {@link ExecutorService}.
     */
    public boolean isShutdown() { return executorService.isShutdown(); }

    /**
     * Returns <tt>true</tt> if all tasks have completed following shut down.
     * Note that <tt>isTerminated</tt> is never <tt>true</tt> unless
     * <tt>shutdown</tt> was called first.
     *
     * wrap for inner {@link ExecutorService}.
     */
    public boolean isTerminated() { return executorService.isTerminated(); }

    /**
     * Blocks until all tasks have completed execution after a shutdown
     * request, or the timeout occurs, or the current thread is
     * interrupted, whichever happens first.
     *
     * wrap for inner {@link ExecutorService}.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return <tt>true</tt> if this executor terminated and
     *         <tt>false</tt> if the timeout elapsed before termination
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executorService.awaitTermination(timeout, unit);
    }

    public static abstract class Computable<V> {
        protected final String name;
        /** specify a name which identity the computation simply **/
        public Computable(String name) {
            if(name == null) { throw new NullPointerException("computable name is null"); }
            this.name = name;
        }
        public abstract V compute() throws Exception;
    }

    private static class Task implements Runnable {
        private static final AtomicReferenceFieldUpdater RUNNER_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(Task.class, Thread.class, "runner");

        private final Computable<?> computable;
        private final int index;
        private final Result result;

        private volatile Thread runner;

        public Task(Computable<?> computable, int index, Result result) {
            this.computable = computable;
            this.index = index;
            this.result = result;
        }

        @Override
        public final void run() {
            if(this.result.getStatus() != Result.INIT ||
                !RUNNER_UPDATER.compareAndSet(this, null, Thread.currentThread())) {
                return;
            }

            boolean ran = false;
            Object result = null;
            Exception ex = null;
            Transaction t = TRANSACTION_FACTORY.newTransaction(this.result.taskType, computable.name);
            t.addData("identity", this.result.identity);
            try {
                result = computable.compute();
                t.setStatus(Transaction.SUCCESS);
                ran = true;
            }catch(Exception e) {
                if(e instanceof InterruptedException) {
                    t.setStatus(Transaction.SUCCESS); // interruption is normal for task cancellation
                }else {
                    t.setStatus(e);
                }
                ex = e;
                ran = false;
            }finally {
                runner = null;
                t.complete();
                setResult(ran, result, ex, t.getDurationInMillis());    // must after runner reset, because may be cancel tasks
            }
        }

        private void setResult(boolean ran, Object result, Exception ex, long timeInMills) {
            if(ran) {
                this.result.setResult(index, result, timeInMills);
            }else {
                this.result.setException(index, ex, timeInMills);
            }
        }
    }

    public static abstract class Result {

        protected static final int INIT = 0;
        protected static final int ONGOING = 1;
        protected static final int SUCCESS = 2;
        protected static final int CANCELLED = 3;
        protected static final int EXCEPTIONAL = 4;

        private final AtomicInteger status;

        final String taskType;    // task type for monitor
        final List<Task> bindTasks;

        private final AtomicLong minDurationInMills;
        private final AtomicLong maxDurationInMills;
        final String identity;
        private long startTime;
        private long submissionDurationInMills;

        Result(String type, List<Task> tasks) {
            status = new AtomicInteger(INIT);
            taskType = type;
            bindTasks = tasks;
            identity = UUID.randomUUID().toString();
            minDurationInMills = new AtomicLong(Long.MAX_VALUE);
            maxDurationInMills = new AtomicLong(Long.MIN_VALUE);
            startTime = System.currentTimeMillis();
            submissionDurationInMills = 0L;
        }

        /**
         * Completion may be due to success termination, an exception, or
         * cancellation -- in all of these cases, this method will return
         * <tt>true</tt>.
         */
        public boolean isAllDone() { return status.get() > ONGOING; }

        /** return <tt>true</tt> if Completion due to cancellation **/
        public boolean isCancelled() { return status.get() == CANCELLED; }

        /**
         * Attempts to cancel execution of this task
         * @return  <tt>false</tt> if the task could not be cancelled,
         * typically because it has already completed normally;
         * <tt>true</tt> otherwise
         */
        public final boolean cancel() {
            if(status.get() != INIT || !status.compareAndSet(INIT, CANCELLED)) {
                return false;
            }
            notifyWaiters();
            cancelTasks();
            completeComputation(CANCELLED);
            return true;
        }

        /** signal all waiting threads **/
        abstract void notifyWaiters();

        final void cancelTasks() {
            List<Task> tasks = bindTasks;
            if(tasks != null) {
                if(!(tasks instanceof RandomAccess)) {
                    tasks = new ArrayList<Task>(tasks);
                }
                int size = tasks.size();
                for(int i=size-1; i>=0; i--) {
                    Thread t = tasks.get(i).runner;
                    if(t != null) {
                        t.interrupt();
                    }
                }
            }
        }

        abstract <V> void setResult(int order, V result, long duration);

        abstract void setException(int order, Exception ex, long duration);

        final int getStatus() { return status.get(); }

        /** compare and update result status **/
        final boolean casStatus(int expect, int update) {
            return status.compareAndSet(expect, update);
        }

        /** Eventually set the result status **/
        final void lazySetStatus(int finalStatus) {
            status.lazySet(finalStatus);
        }

        /** update duration **/
        final void updateDurations(long duration) {
            updateMinDuration(duration);
            updateMaxDuration(duration);
        }

        private void updateMinDuration(long duration) {
            long current;
            do {
                current = minDurationInMills.get();
            }while(current > duration && !minDurationInMills.compareAndSet(current, duration));
        }

        private void updateMaxDuration(long duration) {
            long current;
            do {
                current = maxDurationInMills.get();
            }while(current < duration && !maxDurationInMills.compareAndSet(current, duration));
        }

        final void startSubmission() {
            startTime = System.currentTimeMillis();
        }

        final void completeSubmission() {
            submissionDurationInMills = System.currentTimeMillis() - startTime;
        }

        /** end computation transaction with target <code>resultStatus</code> **/
        final void completeComputation(int resultStatus) {
            // cat Transaction store data into ThreadLocal, so delay Transaction creation
            long timeInMills = System.currentTimeMillis() - startTime;
            Transaction t = TRANSACTION_FACTORY.newTransactionWithDuration(taskType, resultName(), timeInMills);
            t.addData("identity", identity);
            try {
                recordStatistics(t);
                t.addData("resultStatus", resultStatus);
                if(resultStatus == SUCCESS) {
                    t.setStatus(Transaction.SUCCESS);
                }else {
                    t.setStatus(String.valueOf(resultStatus));
                }
            }finally {
                t.complete();
            }
        }

        /** return result identity name for cat transaction logging **/
        String resultName() { return getClass().getSimpleName(); }

        /** add customized statistics into cat transaction **/
        void recordStatistics(Transaction t) {
            t.addData("taskCount", bindTasks.size());
            t.addData("submissionDuration", submissionDurationInMills);
            t.addData("minExecutionDuration", minDurationInMills.get());
            t.addData("maxExecutionDuration", maxDurationInMills.get());
        }
    }

    public static class SingleResult<V> extends Result {
        private final boolean fastFirst;
        private final AtomicInteger exceptionCount;
        private Object result;

        SingleResult(String type, List<Task> tasks, boolean fastFirst) {
            super(type, tasks);
            this.fastFirst = fastFirst;
            this.exceptionCount = new AtomicInteger(0);
        }

        /**
         * Waits if necessary for at most the given time for the completion, and then retrieves its result, if available.
         * @param timeout   the maximum time to wait
         * @param unit  the time unit of the timeout argument
         * @return  the computed result
         * @throws IllegalArgumentException if timeout argument is illegal
         * @throws InterruptedException if the current thread was interrupted while waiting
         * @throws ExecutionException   if the computation threw an exception
         * @throws TimeoutException if the wait timed out
         * @throws CancellationException    if the computation was cancelled
         */
        public V get(long timeout, TimeUnit unit) throws IllegalArgumentException, InterruptedException,
            ExecutionException, TimeoutException, CancellationException {
            if(unit == null || timeout < 0) {
                throw new IllegalArgumentException("illegal timeout waiting");
            }
            if(Thread.interrupted()) { throw new InterruptedException(); }
            int s = getStatus();
            if(s <= ONGOING) {
                long millis = unit.toMillis(timeout);
                long deadline = System.currentTimeMillis() + millis;
                synchronized(this) {
                    while((s = getStatus()) <= ONGOING) {
                        wait(millis);
                        millis = deadline - System.currentTimeMillis();
                        if(millis <= 0) {
                            s = getStatus();
                            break;
                        }
                    }
                }
            }
            if(s <= ONGOING) {
                throw new TimeoutException();
            }else if(s == SUCCESS) {
                return (V) result;
            }else if(s == CANCELLED) {
                throw new CancellationException();
            }else {
                throw new ExecutionException((Throwable)result);
            }
        }

        @Override
        void notifyWaiters() {
            synchronized(this) { notifyAll(); }
        }

        @Override
        <V> void setResult(int order, V result, long duration) {
            if(casStatus(INIT, ONGOING)) {
                this.result = result;
                updateDurations(duration);
                lazySetStatus(SUCCESS);
                notifyWaiters();
                cancelTasks();  // cancel other unfinished tasks
                completeComputation(SUCCESS);
            }
        }

        @Override
        void setException(int order, Exception ex, long duration) {
            if(fastFirst) {
                if(casStatus(INIT, ONGOING)) {
                    this.result = ex;
                    // exception task ignore duration
                    lazySetStatus(EXCEPTIONAL);
                    notifyWaiters();
                    cancelTasks();  // cancel other unfinished tasks
                    completeComputation(EXCEPTIONAL);
                }
            }else {
                int count = exceptionCount.incrementAndGet();
                // the last task also exceptional
                if(count == bindTasks.size() && casStatus(INIT, ONGOING)) {
                    this.result = ex;
                    // exception task ignore duration
                    lazySetStatus(EXCEPTIONAL);
                    notifyWaiters();
                    completeComputation(EXCEPTIONAL);
                }
            }
        }

        @Override
        void recordStatistics(Transaction t) {
            super.recordStatistics(t);
            t.addData("fastFirst", fastFirst);
            if(!fastFirst) {
                t.addData("exceptionCount", exceptionCount.get());
            }
        }

        @Override
        String resultName() {
            return new StringBuilder().append(getClass().getSimpleName()).append("-").append(bindTasks.size()).toString();
        }
    }

    static abstract class ResultSet extends Result {
        private final ResultWrapper[] results;
        private final AtomicInteger successCount;

        ResultSet(String type, List<Task> tasks, int taskCapacity) {
            super(type, tasks);
            results = init(taskCapacity);
            successCount = new AtomicInteger(0);
        }

        private ResultWrapper[] init(int size) {
            ResultWrapper[] results = new ResultWrapper[size];
            for(int i=0; i<size; i++) {
                results[i] = new ResultWrapper();
            }
            return results;
        }

        @Override
        final void notifyWaiters() {
            for(ResultWrapper wrapper : results) {
                synchronized(wrapper) { wrapper.notifyAll(); }
            }
        }

        @Override
        <V> void setResult(int order, V result, long duration) {
            if(getStatus() == INIT) {
                results[order].complete(result);
                updateDurations(duration);
                if (successCount.incrementAndGet() == results.length
                    && casStatus(INIT, SUCCESS)) {
                    completeComputation(SUCCESS);
                }
            }
        }

        @Override
        void setException(int order, Exception ex, long duration) {
            if(casStatus(INIT, EXCEPTIONAL)) {
                results[order].completeAbnormally(ex);
                cancelTasks();  // cancel other unfinished tasks
                completeComputation(EXCEPTIONAL);
            }
        }

        @Override
        void recordStatistics(Transaction t) {
            super.recordStatistics(t);
            t.addData("successCount", successCount.get());
        }

        /** return the <code>index</code>th result from the result set **/
        protected final Object resultAt(int index, long timeout, TimeUnit unit) throws IllegalArgumentException,
            InterruptedException, ExecutionException, TimeoutException, CancellationException {
            if(unit == null || timeout < 0) {
                throw new IllegalArgumentException("illegal timeout waiting");
            }
            if(Thread.interrupted()) { throw new InterruptedException(); }
            int s = getStatus();
            ResultWrapper wrapper = results[index];
            int ws = wrapper.getStatus();
            if(ws <=ONGOING && s <= ONGOING) {
                long millis = unit.toMillis(timeout);
                long deadline = System.currentTimeMillis() + millis;
                synchronized(wrapper) {
                    while((ws = wrapper.getStatus()) <= ONGOING && (s=getStatus()) <=ONGOING) {
                        wrapper.wait(millis);
                        millis = deadline - System.currentTimeMillis();
                        if(millis <= 0) {
                            ws = wrapper.getStatus();
                            s = getStatus();
                            break;
                        }
                    }
                }
            }
            if(ws <= ONGOING) {
                if(s == CANCELLED) {
                    throw new CancellationException();
                }else if(s == EXCEPTIONAL) {
                    throw new CancellationException();  // cancelled by exception from other task
                }else {
                    throw new TimeoutException();
                }
            }else if(ws == SUCCESS) {
                return wrapper.getResult();
            }else {
                throw new ExecutionException((Throwable)wrapper.getResult());
            }
        }
    }

    private static class ResultWrapper {
        volatile int status;
        Object result;
        ResultWrapper() {
            this.status = Result.INIT;
            this.result = null;
        }
        <V> void complete(V result) {
            this.result = result;
            this.status = Result.SUCCESS;   // must after result assignment
            synchronized(this) { notifyAll(); }
        }
        void completeAbnormally(Exception ex) {
            this.result = ex;
            this.status = Result.EXCEPTIONAL;   // must after result assignment
            synchronized(this) { notifyAll(); }
        }
        int getStatus() { return status; }
        Object getResult() { return result; }
    }

    public static class MultiResult<V> extends ResultSet {
        MultiResult(String type, List<Task> tasks, int taskCapacity) {
            super(type, tasks, taskCapacity);
        }

        /**
         * Waits if necessary for at most the given time for the completion, and then retrieves
         * result mapping for the <code>index</code><i>th</i> submitted task, if available.
         * @param timeout   the maximum time to wait
         * @param unit  the time unit of the timeout argument
         * @return  the computed result
         * @throws IllegalArgumentException if timeout argument is illegal
         * @throws InterruptedException if the current thread was interrupted while waiting
         * @throws ExecutionException   if the computation threw an exception
         * @throws TimeoutException if the wait timed out
         * @throws CancellationException    if the computation was cancelled
         */
        public final V get(int index, long timeout, TimeUnit unit) throws IllegalArgumentException,
            InterruptedException, ExecutionException, TimeoutException, CancellationException {
            return (V)resultAt(index, timeout, unit);
        }

        @Override
        String resultName() {
            return new StringBuilder().append(getClass().getSimpleName()).append("-").append(bindTasks.size()).toString();
        }
    }

    public static class PairResult<V1, V2> extends ResultSet {
        PairResult(String type, List<Task> tasks, int taskCapacity) {
            super(type, tasks, taskCapacity);
        }

        /**
         * Waits if necessary for at most the given time for the completion, and then retrieves
         * result mapping for the first submitted task, if available.
         */
        public V1 getFirstResult(long timeout, TimeUnit unit) throws IllegalArgumentException,
            InterruptedException, ExecutionException, TimeoutException, CancellationException {
            return (V1)resultAt(0, timeout, unit);
        }

        /**
         * Waits if necessary for at most the given time for the completion, and then retrieves
         * result mapping for the second submitted task, if available.
         */
        public V2 getSecondResult(long timeout, TimeUnit unit) throws IllegalArgumentException,
            InterruptedException, ExecutionException, TimeoutException, CancellationException {
            return (V2)resultAt(1, timeout, unit);
        }
    }

    public static class TripleResult<V1, V2, V3> extends PairResult<V1, V2> {
        TripleResult(String type, List<Task> tasks, int taskCapacity) {
            super(type, tasks, taskCapacity);
        }

        /**
         * Waits if necessary for at most the given time for the completion, and then retrieves
         * result mapping for the third submitted task, if available.
         */
        public V3 getThirdResult(long timeout, TimeUnit unit) throws IllegalArgumentException,
            InterruptedException, ExecutionException, TimeoutException, CancellationException {
            return (V3)resultAt(2, timeout, unit);
        }
    }

    public static class FourResult<V1, V2, V3, V4> extends TripleResult<V1, V2, V3> {
        FourResult(String type, List<Task> tasks, int taskCapacity) {
            super(type, tasks, taskCapacity);
        }

        /**
         * Waits if necessary for at most the given time for the completion, and then retrieves
         * result mapping for the fourth submitted task, if available.
         */
        public V4 getFourthResult(long timeout, TimeUnit unit) throws IllegalArgumentException,
            InterruptedException, ExecutionException, TimeoutException, CancellationException {
            return (V4)resultAt(3, timeout, unit);
        }
    }

    public static class FiveResult<V1, V2, V3, V4, V5> extends FourResult<V1, V2, V3, V4> {
        FiveResult(String type, List<Task> tasks, int taskCapacity) {
            super(type, tasks, taskCapacity);
        }

        /**
         * Waits if necessary for at most the given time for the completion, and then retrieves
         * result mapping for the fifth submitted task, if available.
         */
        public V5 getFifthResult(long timeout, TimeUnit unit) throws IllegalArgumentException,
            InterruptedException, ExecutionException, TimeoutException, CancellationException {
            return (V5)resultAt(4, timeout, unit);
        }
    }

    public static class TupleResult extends ResultSet {
        TupleResult(String type, List<Task> tasks, int taskCapacity) {
            super(type, tasks, taskCapacity);
        }

        /**
         * Waits if necessary for at most the given time for the completion, and then retrieves
         * result mapping for the <code>index</code><i>th</i> submitted task, if available.
         * @param index the result index
         * @param timeout   the maximum time to wait
         * @param unit  the time unit of the timeout argument
         * @return  the computed result
         * @throws IllegalArgumentException if timeout argument is illegal
         * @throws InterruptedException if the current thread was interrupted while waiting
         * @throws ExecutionException   if the computation threw an exception
         * @throws TimeoutException if the wait timed out
         * @throws CancellationException    if the computation was cancelled
         */
        public <V> V get(int index, long timeout, TimeUnit unit) throws IllegalArgumentException,
            InterruptedException, ExecutionException, TimeoutException, CancellationException {
            return (V)resultAt(index, timeout, unit);
        }

        @Override
        String resultName() {
            return new StringBuilder().append(getClass().getSimpleName()).append("-").append(bindTasks.size()).toString();
        }
    }
    
    /** record execution duration of a piece of code **/
    static interface Transaction {
    	
    	String SUCCESS = "0";
    	
    	/** add key-value pair **/
    	void addData(String key, Object value);
    	
    	/** set the transaction status. "0" represents success, otherwise failed. **/
    	void setStatus(String status);
    	
    	/** set the transaction status with exception **/
    	void setStatus(Throwable t);
    	
    	/** complete the construction **/
    	void complete();
    	
    	/** How long the transaction took from construction to complete. Time unit is microsecond **/
    	long getDurationInMillis();
    }
    
    interface TransactionFactory {
    	
    	/** create a new transaction with given type and name **/
    	Transaction newTransaction(String type, String name);
    	
    	/** create a new transaction with given type, name and duration **/
    	Transaction newTransactionWithDuration(String type, String name, long durationInMills);
    }
    
    private static class DefaultTransaction implements Transaction {
    	private final String type;
    	private final String name;
    	private final long nanoTime;
    	private long durationInMills;
    	private StringBuilder dataBuf;
    	private Object status;
    	
    	DefaultTransaction(String type, String name) {
    		this.type = type;
    		this.name = name;
    		this.nanoTime = System.nanoTime();
    	}
    	
    	DefaultTransaction(String type, String name, long durationInMills) {
    		this.type = type;
    		this.name = name;
    		this.nanoTime = System.nanoTime();
    		this.durationInMills = durationInMills;
    	}
    	
		@Override
		public void addData(String key, Object value) {
			if(dataBuf == null) {
				dataBuf = new StringBuilder();
			}else {
				dataBuf.append("&");
			}
			dataBuf.append(key).append("=").append(value);
		}

		@Override
		public void setStatus(String status) {
			this.status = status;
		}

		@Override
		public void setStatus(Throwable t) {
			this.status = t;
		}

		@Override
		public void complete() {
			int capacity = (type == null ? 0 : type.length())
					+ (name == null ? 0 : name.length())
					+ (dataBuf == null ? 0 : dataBuf.length())
					+ 128;
			StringBuilder buf = new StringBuilder(capacity);
			buf.append("Transaction(type=").append(type).append(", name=").append(name).append("):\n")
				.append("data: {").append(dataBuf).append("}\n")
				.append("duration: ").append(durationInMills).append("\n");
			if(status instanceof Throwable) {
				System.out.println(buf);
				Throwable t = (Throwable) status;
				t.printStackTrace(System.out);
			}else {
				buf.append("status: ").append(status).append("\n");
				System.out.println(buf);
			}
		}

		@Override
		public long getDurationInMillis() {
			if(durationInMills == 0) {
				durationInMills = (System.nanoTime() - nanoTime + 500000) / 1000000;
			}
			return durationInMills;
		}
    }
    
    public static class DefaultTransactionFactory implements TransactionFactory {
		@Override
		public Transaction newTransaction(String type, String name) {
			return new DefaultTransaction(type, name);
		}

		@Override
		public Transaction newTransactionWithDuration(String type, String name, long durationInMills) {
			return new DefaultTransaction(type, name, durationInMills);
		}
    }
}
