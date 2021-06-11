package com.mgg.schedule.message;

/**
 * 任务模型
 */
public class Work {

    Long id;

    Double executeTime;

    public Work() {

    }

    public Work(Long id, Double executeTime) {
        this.id = id;
        this.executeTime = executeTime;
    }

    public Long getId() {
        return id;
    }

    public Work setId(Long id) {
        this.id = id;
        return this;
    }

    public Double getExecuteTime() {
        return executeTime;
    }

    public Work setExecuteTime(Double executeTime) {
        this.executeTime = executeTime;
        return this;
    }

    @Override
    public String toString() {
        return "Work{" +
                "id=" + id +
                ", executeTime=" + executeTime +
                '}';
    }
}
