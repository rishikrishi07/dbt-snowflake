import logging
import time
import json
import os
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, Optional, Union

from airflow.models import TaskInstance
from airflow.utils.logger import AirflowTaskLogger, task_logger


class AirflowTaskLogger:
    """
    A generic logger for Airflow tasks that provides standard logging patterns
    and execution metrics tracking.
    """
    
    def __init__(self, 
                 task_instance: Optional[TaskInstance] = None,
                 dag_id: Optional[str] = None,
                 task_id: Optional[str] = None,
                 log_level: int = logging.INFO):
        """
        Initialize the logger with task context.
        
        Args:
            task_instance: Airflow TaskInstance object (preferred)
            dag_id: DAG ID if TaskInstance not available
            task_id: Task ID if TaskInstance not available
            log_level: Logging level (default: INFO)
        """
        self.task_instance = task_instance
        
        # Extract context from TaskInstance or use provided values
        if task_instance:
            self.dag_id = task_instance.dag_id
            self.task_id = task_instance.task_id
            self.run_id = task_instance.run_id
            self.execution_date = task_instance.execution_date
        else:
            self.dag_id = dag_id or "unknown_dag"
            self.task_id = task_id or "unknown_task"
            self.run_id = "manual"
            self.execution_date = datetime.now()
        
        # Set up logging
        self.logger = logging.getLogger(f"{self.dag_id}.{self.task_id}")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        self.logger.setLevel(log_level)
        
        # Execution metrics
        self.start_time = None
        self.end_time = None
        self.execution_time = None
        self.metrics = {}
    
    def start_task(self, additional_context: Optional[Dict[str, Any]] = None) -> None:
        """
        Log the start of a task with context information.
        
        Args:
            additional_context: Additional context to log
        """
        self.start_time = time.time()
        
        context = {
            "event": "task_start",
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat()
        }
        
        if additional_context:
            context.update(additional_context)
        
        self.logger.info(f"Starting task {self.task_id} in DAG {self.dag_id}")
        self.logger.debug(f"Task context: {json.dumps(context)}")
    
    def end_task(self, status: str = "success", additional_metrics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Log the end of a task with execution metrics.
        
        Args:
            status: Task status (success, failure, etc.)
            additional_metrics: Additional metrics to log
            
        Returns:
            Dictionary with execution metrics
        """
        self.end_time = time.time()
        self.execution_time = self.end_time - self.start_time
        
        metrics = {
            "event": "task_end",
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "run_id": self.run_id,
            "status": status,
            "execution_time_seconds": round(self.execution_time, 3),
            "timestamp": datetime.now().isoformat()
        }
        
        if additional_metrics:
            metrics.update(additional_metrics)
        
        self.metrics = metrics
        
        self.logger.info(f"Task {self.task_id} completed with status: {status}")
        self.logger.info(f"Execution time: {self.execution_time:.3f} seconds")
        self.logger.debug(f"Task metrics: {json.dumps(metrics)}")
        
        return metrics
    
    def log_step(self, step_name: str, additional_context: Optional[Dict[str, Any]] = None) -> None:
        """
        Log a step within a task.
        
        Args:
            step_name: Name of the step
            additional_context: Additional context to log
        """
        context = {
            "event": "step_execution",
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "step": step_name,
            "timestamp": datetime.now().isoformat()
        }
        
        if additional_context:
            context.update(additional_context)
        
        self.logger.info(f"Executing step: {step_name}")
        self.logger.debug(f"Step context: {json.dumps(context)}")
    
    def log_dbt_output(self, dbt_output: Union[str, Dict[str, Any]], level: int = logging.INFO) -> None:
        """
        Log dbt command output with appropriate formatting.
        
        Args:
            dbt_output: Output from dbt command (string or dict)
            level: Logging level for this output
        """
        if isinstance(dbt_output, dict):
            self.logger.log(level, f"DBT Output: {json.dumps(dbt_output, indent=2)}")
        else:
            # Process string output line by line
            for line in str(dbt_output).split('\n'):
                if line.strip():
                    self.logger.log(level, f"DBT: {line.strip()}")
    
    def log_error(self, error: Exception, context: Optional[Dict[str, Any]] = None) -> None:
        """
        Log an error with context.
        
        Args:
            error: Exception object
            context: Additional context for the error
        """
        error_info = {
            "event": "error",
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": datetime.now().isoformat()
        }
        
        if context:
            error_info.update(context)
        
        self.logger.error(f"Error in task {self.task_id}: {str(error)}")
        self.logger.debug(f"Error context: {json.dumps(error_info)}")


def task_logger(func: Callable) -> Callable:
    """
    Decorator to automatically log task execution with metrics.
    
    Example:
        @task_logger
        def my_task(**context):
            # Task implementation
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        context = kwargs.get('context', {})
        ti = context.get('ti') if context else None
        
        # Extract dag_id and task_id from context if available
        dag_id = context.get('dag').dag_id if context and 'dag' in context else None
        task_id = context.get('task').task_id if context and 'task' in context else None
        
        logger = AirflowTaskLogger(task_instance=ti, dag_id=dag_id, task_id=task_id)
        logger.start_task()
        
        try:
            result = func(*args, **kwargs)
            logger.end_task(status="success")
            return result
        except Exception as e:
            logger.log_error(e)
            logger.end_task(status="failure")
            raise
    
    return wrapper


# Example of how to use the logger with dbt output
def log_dbt_results(ti: TaskInstance, dbt_result: Dict[str, Any]) -> None:
    """
    Utility function to log dbt results from a TaskInstance
    
    Args:
        ti: TaskInstance object
        dbt_result: DBT result object
    """
    logger = AirflowTaskLogger(task_instance=ti)
    
    # Extract metrics from dbt result
    metrics = {
        "total_models": len(dbt_result.get("results", [])),
        "success_count": sum(1 for r in dbt_result.get("results", []) if r.get("status") == "success"),
        "error_count": sum(1 for r in dbt_result.get("results", []) if r.get("status") == "error"),
        "skipped_count": sum(1 for r in dbt_result.get("results", []) if r.get("status") == "skipped"),
        "execution_time": dbt_result.get("elapsed_time", 0)
    }
    
    logger.log_step("dbt_results", metrics)
    
    # Log detailed results
    for result in dbt_result.get("results", []):
        if result.get("status") == "error":
            logger.logger.error(f"DBT model {result.get('unique_id')} failed: {result.get('message')}")
        else:
            logger.logger.info(f"DBT model {result.get('unique_id')} status: {result.get('status')}") 