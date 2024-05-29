package com.zdf.worker.test;

import com.zdf.worker.Client.TaskFlower;
import com.zdf.worker.Client.TaskFlowerImpl;
import com.zdf.worker.data.AsyncFlowClientData;
import com.zdf.worker.data.AsyncTaskRequest;
import com.zdf.worker.data.AsyncTaskReturn;
import com.zdf.worker.data.AsyncTaskSetRequest;
import com.zdf.worker.enums.TaskStatus;
import com.zdf.worker.task.Lark;
import com.zdf.worker.task.TaskBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


@RestController
@RequestMapping("/test")
public class Test {
    static TaskFlower taskFlower = new TaskFlowerImpl();
    public static void main(String[] args) {
        // 用于测试创建任务
        testCeateTask();
        testSetTask();

    }
    @GetMapping("/createTask")
    public void createTask(){
        testCeateTask();
    }



    private static void testGetTaskList() {
        List<AsyncTaskReturn> larkTask = taskFlower.getTaskList(Lark.class, 1, 5);
        System.out.println(larkTask);
    }

    private static void testSetTask() {
//        AsyncTaskSetRequest asyncTaskSetRequest = AsyncTaskSetRequest.builder();
//
//        asyncTaskSetRequest.setStatus(TaskStatus.PENDING.getStatus());
//        taskFlower.setTask(asyncTaskSetRequest);
    }

    private static void testGetTask() {
        AsyncTaskReturn task = taskFlower.getTask("123");
        System.out.println(task);
    }

    private static String testCeateTask() {
        AsyncFlowClientData asyncFlowClientData = null;
        try {
            asyncFlowClientData = TaskBuilder.build(new Lark());
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        String task = taskFlower.createTask(new AsyncTaskRequest(asyncFlowClientData));
        return task;
    }

    private static void createTaskConfig() {

    }
}
