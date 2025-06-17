# utils/logger_utils.py
# 각 스레드당 따로 log 
import os
import logging
from datetime import datetime

def get_thread_logger(location, keyword, thread_id=None, enable_logging=True):
    logger = logging.getLogger(f"{location}-{keyword}-thread{thread_id}")
    logger.setLevel(logging.INFO if enable_logging else logging.CRITICAL)

    if hasattr(logger, "_initialized") and logger._initialized:
        return logger


    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)  

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

    # File handler
    file_handler = logging.FileHandler(f"logs/{location}-{keyword}-thread{thread_id}.log", mode='a')
    file_handler.setFormatter(formatter)

    # Stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    if enable_logging:
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    logger._initialized = True
    logger._file_handler = file_handler
    logger._stream_handler = stream_handler
    return logger
