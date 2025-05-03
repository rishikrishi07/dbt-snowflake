import logging
import time
from functools import wraps
from datetime import datetime


class DbtLogger:
    """
    A lightweight logger for dbt operations in Airflow
    """
    
    def __init__(self, dag_id=None, task_id=None):
        """Initialize the logger with context"""
        self.dag_id = dag_id
        self.task_id = task_id
        self.start_time = None
        self.logger = logging.getLogger(f"{dag_id}.{task_id}" if dag_id and task_id else __name__)
    
    def log_start(self, context=None):
        """Log the start of an operation with optional context"""
        self.start_time = time.time()
        msg = f"Starting {'task' if self.task_id else 'operation'}"
        if self.task_id:
            msg += f" {self.task_id}"
        if self.dag_id:
            msg += f" in DAG {self.dag_id}"
        if context:
            msg += f" - {context}"
        self.logger.info(msg)
        return self.start_time
    
    def log_end(self, status="success"):
        """Log the end of an operation with timing"""
        if not self.start_time:
            self.logger.warning("log_end() called without log_start()")
            duration = 0
        else:
            duration = time.time() - self.start_time
        
        msg = f"Finished {'task' if self.task_id else 'operation'}"
        if self.task_id:
            msg += f" {self.task_id}"
        msg += f" with status {status} in {duration:.2f} seconds"
        self.logger.info(msg)
        return duration
    
    def log_step(self, step_name):
        """Log a step within an operation"""
        self.logger.info(f"Executing: {step_name}")
    
    def log_error(self, error):
        """Log an error with context"""
        self.logger.error(f"Error {'in task ' + self.task_id if self.task_id else ''}: {str(error)}")
    
    def log_dbt(self, message, level=logging.INFO):
        """Log dbt-specific output"""
        self.logger.log(level, f"DBT: {message}")


def log_task(func):
    """
    Simple decorator to log task execution
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract context if available
        context = kwargs.get('context', {})
        ti = context.get('ti') if context else None
        
        # Get task and dag ids if available
        dag_id = None
        task_id = None
        if ti:
            dag_id = ti.dag_id
            task_id = ti.task_id
        elif context and 'task' in context:
            task_id = context['task'].task_id
            dag_id = context['task'].dag_id if hasattr(context['task'], 'dag_id') else None
        
        # Create logger and log start
        logger = DbtLogger(dag_id=dag_id, task_id=task_id)
        logger.log_start()
        
        try:
            # Execute the function
            result = func(*args, **kwargs)
            # Log successful completion
            logger.log_end("success")
            return result
        except Exception as e:
            # Log error and end
            logger.log_error(e)
            logger.log_end("failed")
            raise
    
    return wrapper


# Helper functions
def log_dbt_result(result, dag_id=None, task_id=None):
    """Helper function to log dbt results"""
    logger = DbtLogger(dag_id=dag_id, task_id=task_id)
    
    if not result:
        logger.logger.warning("No dbt result to log")
        return
    
    if isinstance(result, dict):
        # If it's a structured result
        if 'results' in result:
            success_count = sum(1 for r in result['results'] if r.get('status') == 'success')
            error_count = sum(1 for r in result['results'] if r.get('status') == 'error')
            total = len(result['results'])
            logger.logger.info(f"DBT completed: {success_count}/{total} successful, {error_count}/{total} failed")
        else:
            logger.logger.info(f"DBT completed with result: {result}")
    else:
        # For string output, just log it
        logger.log_dbt(str(result)) 