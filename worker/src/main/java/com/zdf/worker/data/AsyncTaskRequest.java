package com.zdf.worker.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 请求体封装类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AsyncTaskRequest {
    AsyncFlowClientData taskData;
}
