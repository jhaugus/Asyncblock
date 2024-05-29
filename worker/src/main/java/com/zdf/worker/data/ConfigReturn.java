package com.zdf.worker.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 数据库封装类，t_schedule_cfg
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConfigReturn {
    List<ScheduleConfig> scheduleCfgList;
}
