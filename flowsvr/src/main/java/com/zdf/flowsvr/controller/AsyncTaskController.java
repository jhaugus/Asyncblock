package com.zdf.flowsvr.controller;

import com.zdf.flowsvr.constant.ErrorStatusReturn;
import com.zdf.flowsvr.data.AsyncTaskRequest;
import com.zdf.flowsvr.data.AsyncTaskSetRequest;
import com.zdf.flowsvr.data.ReturnStatus;
import com.zdf.flowsvr.service.AsyncTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import static com.zdf.flowsvr.util.Utils.isStrNull;

@RestController
@RequestMapping("/task")
public class AsyncTaskController {
    @Autowired
    private AsyncTaskService asyncTaskService;

    Logger logger = LoggerFactory.getLogger(AsyncTaskController.class);

    /**
     * 创建一个asyncflow任务
     * 其中，传入参数为一个AsyncFlowClientData对象
     * 同时，创建成功的条件为task_type在t_schedule_pos表和t_schedule_cfg表中存在
     * @param asyncTaskGroup
     * @return
     */
    @PostMapping("/create_task")
    public ReturnStatus createTask(@RequestBody AsyncTaskRequest asyncTaskGroup) {
        if (isStrNull(asyncTaskGroup.getTaskData().getTask_type())){
            logger.error("input invalid");
            return ErrorStatusReturn.ERR_INPUT_INVALID;
        }
        return asyncTaskService.createTask(asyncTaskGroup);
    }

    /**
     * 根据task_id获取任务信息
     * @param task_id
     * @return
     */
    @GetMapping("/get_task")
    public ReturnStatus getTask(@RequestParam("task_id") String task_id) {
        if (isStrNull(task_id)){
            logger.error("input invalid");
            return ErrorStatusReturn.ERR_INPUT_INVALID;
        }
        return asyncTaskService.getTask(task_id);
    }


    /**
     * 根据task_type和status在t_lark_task_1表中查找最多limit条数据。
     * @param taskType
     * @param status
     * @param limit
     * @return
     */
    @GetMapping("/task_list")
    public ReturnStatus getTaskList(@RequestParam("task_type") String taskType, @RequestParam("status") int status, @RequestParam("limit") int limit) {
        if (isStrNull(taskType) || !ErrorStatusReturn.IsValidStatus(status)) {
                logger.error("input invalid");
            return ErrorStatusReturn.ERR_INPUT_INVALID;
        }
        return asyncTaskService.getTaskList(taskType, status, limit);
    }

    /**
     * 返回满足task_type和status以及如下过滤的数据
     * filter(asyncFlowTask -> asyncFlowTask.getCrt_retry_num() == 0 || asyncFlowTask.getMax_retry_interval() != 0
     *                         && asyncFlowTask.getOrder_time() <= System.currentTimeMillis()).collect(Collectors.toList());
     * @param taskType
     * @param status
     * @param limit
     * @return
     */
    @GetMapping("/hold_task")
    public ReturnStatus holdTask(@RequestParam("task_type") String taskType, @RequestParam("status") int status, @RequestParam("limit") int limit) {
        if (isStrNull(taskType) || !ErrorStatusReturn.IsValidStatus(status)) {
            logger.error("input invalid");
            return ErrorStatusReturn.ERR_INPUT_INVALID;
        }
        return asyncTaskService.holdTask(taskType, status, limit);
    }


    /**
     * 根据task_id设置任务状态
     * @param asyncTaskSetRequest
     * @return
     */
    @PostMapping("/set_task")
    public ReturnStatus addTask(@RequestBody AsyncTaskSetRequest asyncTaskSetRequest) {
        if (isStrNull(asyncTaskSetRequest.getTask_id())) {
            logger.error("input invalid");
            return ErrorStatusReturn.ERR_INPUT_INVALID;
        }
        return asyncTaskService.setTask(asyncTaskSetRequest);
    }


    /**
     * 根据user_id和status_list获取用户列表
     * @param user_id
     * @param statusList
     * @return
     */
    @GetMapping("/user_task_list")
    public ReturnStatus getUserTaskList(@RequestParam("user_id") String user_id, @RequestParam("status_list") int statusList) {
        if (isStrNull(user_id)) {
            logger.error("input invalid");
            return ErrorStatusReturn.ERR_INPUT_INVALID;
        }
        return asyncTaskService.getTaskByUserIdAndStatus(user_id, statusList);
    }



}
