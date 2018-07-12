package com.unistack.tamboo.message.kafka.storage;


import com.unistack.tamboo.message.kafka.runtime.RunnerConfig;
import com.unistack.tamboo.message.kafka.runtime.RunnerStatus;


/**
 * @author Gyges Zean
 * @date 2018/4/23
 * storage for connector status
 */
public interface StatusBackingStore {

    /**
     * Start dependent services (if needed)
     */
    void start();


    /**
     * Stop dependent services (if needed)
     */
    void stop();

    /**
     * Set the state of the connector to the given value.
     * @param status the status of the task
     */
    void put(RunnerStatus status);


    /**
     * Get the current state of the task.
     * @param runner the id of the task
     * @return the state or null if there is none
     */
    RunnerStatus get(String runner);

    /**
     * Flush any pending writes
     */
    void flush();

    /**
     * Configure class with the given key-value pairs
     * @param config config for StatusBackingStore
     */
    void configure(RunnerConfig config);
}
