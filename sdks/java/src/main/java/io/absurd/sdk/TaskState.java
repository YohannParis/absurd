package io.absurd.sdk;

/**
 * Represents the possible states of a task in the Absurd workflow system.
 */
public enum TaskState {
    /** Task is waiting to be executed */
    PENDING,
    
    /** Task is currently being executed */
    RUNNING,
    
    /** Task completed successfully */
    COMPLETED,
    
    /** Task failed to complete */
    FAILED,
    
    /** Task was cancelled before completion */
    CANCELLED,
    
    /** Task timed out */
    TIMED_OUT,

    /** Task is sleeping, waiting for a scheduled wake time or event signal */
    SLEEPING
}
