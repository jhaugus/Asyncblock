package com.zdf.worker.boot;

import com.zdf.worker.lock.LockParam;
import com.zdf.worker.lock.RedisLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zdf.worker.Client.TaskFlower;
import com.zdf.worker.Client.TaskFlowerImpl;
import com.zdf.worker.constant.TaskConstant;
import com.zdf.worker.constant.UserConfig;
import com.zdf.worker.core.ObserverManager;
import com.zdf.worker.core.observers.TimeObserver;
import com.zdf.worker.data.AsyncTaskBase;
import com.zdf.worker.data.AsyncTaskReturn;
import com.zdf.worker.data.AsyncTaskSetStage;
import com.zdf.worker.data.ScheduleConfig;
import com.zdf.worker.enums.TaskStatus;
import com.zdf.worker.task.Lark;
import com.zdf.worker.task.TaskBuilder;
import com.zdf.worker.task.TaskRet;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 启动器：
 * 1.初始化，获取配置信息表 .get_t_schedule_cfg ==》 scheduleCfgDic， 并且 loadPool 定时更新 scheduleCfgDic
 *
 */
public class AppLaunch implements Launch{
    final TaskFlower taskFlower;//用于发送请求

    public static String packageName; //要执行的类的包名

    // 拉取哪几类任务
    static Class taskType;

    // 拉取哪个任务的指针
    AtomicInteger offset;

    // 观察者模式的观察管理者
    ObserverManager observerManager;
    private Long intervalTime;//请求间隔时间，读取用户配置
    private int scheduleLimit; //一次拉取多少个任务，用户配置
    public Long cycleScheduleConfigTime = 10000L;// 多长时间拉取一次任务配置信息
    public static int MaxConcurrentRunTimes = 5; // 线程池最大数量
    public static int concurrentRunTimes = MaxConcurrentRunTimes; // 线程并发数
    private static String LOCK_KEY = "lock"; // 分布式锁的键
    Map<String, ScheduleConfig> scheduleCfgDic; // 存储任务配置信息
    Logger logger = LoggerFactory.getLogger(AppLaunch.class); //打印日志
    ThreadPoolExecutor threadPoolExecutor; // 拉取任务的线程池
    ScheduledExecutorService loadPool;  // 定时更新配置信息
    // 拉取任务配置信息
    private void loadCfg() {
        List<ScheduleConfig> taskTypeCfgList = taskFlower.getTaskTypeCfgList();
        for (ScheduleConfig scheduleConfig : taskTypeCfgList) {
            scheduleCfgDic.put(scheduleConfig.getTask_type(), scheduleConfig);
        }
    }

    /**
     * 1.初始化时更新配置信息表 scheduleCfgDic
     * 2.定时更新配置信息表 scheduleCfgDic
     * @return
     */
    @Override
    public int init() {
        loadCfg();
        if (scheduleLimit != 0) {
            logger.debug("init ScheduleLimit : %d", scheduleLimit);
            concurrentRunTimes = scheduleLimit;
            MaxConcurrentRunTimes = scheduleLimit;
        } else {
            this.scheduleLimit = this.scheduleCfgDic.get(taskType.getSimpleName()).getSchedule_limit();
        }
        // 定期更新任务配置信息
        loadPool.scheduleAtFixedRate(this::loadCfg, cycleScheduleConfigTime, cycleScheduleConfigTime, TimeUnit.MILLISECONDS);
        return 0;
    }

    public AppLaunch() {
        this(0);
    }
    public AppLaunch(int scheduleLimit) {
        scheduleCfgDic = new ConcurrentHashMap<>();

        loadPool = Executors.newScheduledThreadPool(1);
        taskFlower = new TaskFlowerImpl();
        taskType = Lark.class;
        packageName = taskType.getPackage().getName();
        this.scheduleLimit = scheduleLimit;
        observerManager = new ObserverManager();
        // 向观察管理者注册观察者
        observerManager.registerEventObserver(new TimeObserver());
        offset = new AtomicInteger(0);
        // 初始化，拉取任务配置信息
        init();

    }

    // 启动：拉取任务

    /**
     * 1.初始化，从配置表获取配置信息，设置线程池配置，设置任务类型，以及其他基本信息
     * 2.获取 想要执行任务的表名 taskType
     * 3.开启多线程
     * 4.开始执行
     *  执行流程为：
     *      1.拉取任务表接口，即有相应接口且在等待执行状态的任务
     *      2.如果有需要执行的任务，开始执行
     *      3.将任务置于执行中状态
     *      4.执行成功，则置于成功
     *      5.执行失败，则置于失败
     * @return
     */
    @Override
    public int start() {
        // 1.读取对应任务配置信息
        ScheduleConfig scheduleConfig = scheduleCfgDic.get(taskType.getSimpleName());
        // 2.如果用户没有配置时间间隔就使用默认时间间隔
        intervalTime = scheduleConfig.getSchedule_interval() == 0 ? TaskConstant.DEFAULT_TIME_INTERVAL * 1000L : scheduleConfig.getSchedule_interval() * 1000L;
        // 3.多线程
        this.threadPoolExecutor = new ThreadPoolExecutor(concurrentRunTimes, MaxConcurrentRunTimes, intervalTime + 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(UserConfig.QUEUE_SIZE));
        for(;;) {
            // 4.如果队列不满，则执行，一开始队列长度为 0，每次执行limit条待执行的任务，如果不报错就一直在执行
            if (UserConfig.QUEUE_SIZE - threadPoolExecutor.getQueue().size() >= scheduleLimit) {
                execute(taskType);
            }
            try {
                Thread.sleep(intervalTime + (int)(Math.random() * 500));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
//        for (int i = 0; i < concurrentRunTimes; i++) {
//            // 前后波动500ms
//            int step = (int) (Math.random() * 500 + 1);
//            // 拉取任务
//            threadPoolExecutor.scheduleAtFixedRate(this::execute, step * 3L, intervalTime + step, TimeUnit.MILLISECONDS);
//        }
    }

    // 执行任务

    /**
     * 1.获取任务接口，如果有需要执行的任务就执行任务execute
     * @param taskType
     */
    public void execute(Class<?> taskType) {
        // 1.拉取任务  如果没有任务，或者没有执行成功的任务时，返回null
        List<AsyncTaskBase> asyncTaskBaseList = scheduleTask(taskType);
        if (asyncTaskBaseList == null) {
            return;
        }
        // 2.获取有Bean的任务的数量
        int size = asyncTaskBaseList.size();
        for (int i = 0; i < size; i++) {
            int finalI = i;
            threadPoolExecutor.execute(() -> executeTask(asyncTaskBaseList, finalI));
        }
    }

    // 拉取任务
    /**
     * 1.开始执行前的准备，现在只打印了 开始了
     * 2.获取需要执行任务的接口 getAsyncTaskBases
     * @param taskType
     * @return
     */
    private List<AsyncTaskBase> scheduleTask(Class<?> taskType) {
        try {
            // 1.开始执行时，做点事，这里就是简单的打印了一句话，供后续扩展使用
            observerManager.wakeupObserver(ObserverType.onBoot);
        } catch (InvocationTargetException | IllegalAccessException e) {
            e.printStackTrace();
        }

        // 2.调用拉取任务接口拉取任务
        List<AsyncTaskBase> asyncTaskBaseList = getAsyncTaskBases(observerManager, taskType);
        // 3.为空判断，如果没有任务，或者 没有有Bean的任务，返回null
        if (asyncTaskBaseList == null || asyncTaskBaseList.size() == 0) {
            return null;
        }
        // 4.否则返回 asyncTaskBaseList
        return asyncTaskBaseList;
    }

    /**
     * 1.根据任务类型，从任务表中拉取待执行的任务，通过反射调用方法进行执行
     * 2.当没有任务需要执行，返回null
     * 3.任务执行成功，返回 asyncTaskBaseList，asyncTaskBaseList为需要执行任务的Bean的列表
     * @param observerManager
     * @param taskType
     * @return
     */
    private List<AsyncTaskBase> getAsyncTaskBases(ObserverManager observerManager, Class<?> taskType) {
        // 1.分布式锁的参数
        LockParam lockParam = new LockParam(LOCK_KEY);
        // 2.分布式锁
        RedisLock redisLock = new RedisLock(lockParam);
        List<AsyncTaskReturn> taskList = null;
        try {
            // 3.上锁
            if (redisLock.lock()) {
                // 4.调用http请求接口， 获取t_xxx_task表中的 待执行 的数据， 一次拉取 getSchedule_limit 条
                taskList = taskFlower.getTaskList(taskType, TaskStatus.PENDING.getStatus(), scheduleCfgDic.get(taskType.getSimpleName()).getSchedule_limit());
                if (taskList == null || taskList.size() == 0) {
                    logger.warn("no task to deal!!!!!!!!");
                    logger.warn(redisLock.toString());
                    return null;
                }
                try {
                    // 5.获取相对的任务
                    List<AsyncTaskBase> asyncTaskBaseList = new ArrayList<>();
                    observerManager.wakeupObserver(ObserverType.onObtain, taskList, asyncTaskBaseList);
                    logger.warn(redisLock.toString());
                    return asyncTaskBaseList;
                } catch (InvocationTargetException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            // 6.释放锁
            redisLock.unlock();
        }

        return null;
    }

    // 执行任务

    /**
     * 执行任务
     * 1.将任务设为执行中 ==》 任务执行成功 or 任务执行失败 ==> 将任务设为成功 or 失败
     * @param asyncTaskBaseList
     * @param i
     */
    private void executeTask(List<AsyncTaskBase> asyncTaskBaseList, int i) {
        // 1.通过反射将任务设为执行中
        AsyncTaskBase v = asyncTaskBaseList.get(i);
        try {
            // 执行前干点事，这里就打印了一句话，后续可以扩展，
            observerManager.wakeupObserver(ObserverType.onExecute, v);
        } catch (InvocationTargetException | IllegalAccessException e) {
            e.printStackTrace();
        }

        // 2.通过反射，将任务开始执行
        AsyncTaskSetStage asyncTaskSetStage = null;
        Class<?> aClass = null;
        try {
            // 利用Java反射执行本地方法
            aClass = getaClass(v.getTask_type());
            Method method = TaskBuilder.getMethod(aClass, v.getTask_stage(), v.getTask_context().getParams(), v.getTask_context().getClazz());
            System.out.println(method.getName());
            TaskRet returnVal = (TaskRet) method.invoke(aClass.newInstance(), v.getTask_context().getParams());
            if (returnVal != null) {
                asyncTaskSetStage = returnVal.getAsyncTaskSetStage();
                Object result = returnVal.getResult();
                System.out.println("执行结果为：" + result);
            }
        } catch (Exception e) {
            try {
                // 3.执行出现异常了（任务执行失败了）更改任务状态为PENDING，重试次数+1，超过重试次数设置为FAIL
                observerManager.wakeupObserver(ObserverType.onError, v, scheduleCfgDic.get(v.getTask_type()), asyncTaskBaseList, aClass, e);
                return;
            } catch (InvocationTargetException | IllegalAccessException ex) {
                ex.printStackTrace();
            }
        }


        try {
            // 4.正常执行成功了干点事，方便后续扩展
            observerManager.wakeupObserver(ObserverType.onFinish, v, asyncTaskSetStage, aClass);
        } catch (InvocationTargetException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public Class<?> getaClass(String taskType) throws ClassNotFoundException {
        Class<?> aClass = Class.forName(packageName + "." + taskType);
        return aClass;
    }


    @Override
    public int destroy() {
        return 0;
    }


    // 枚举
    public enum ObserverType {
        onBoot(0),
        onError(1),
        onExecute(2),
        onFinish(3),
        onStop(4), onObtain(5);
        private int code;

        private ObserverType(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }
    }
}
