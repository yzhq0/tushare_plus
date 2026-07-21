# Tushare Plus
# 增强版Tushare API客户端

from .client import (
    APILimitDetector,
    APIResponseError,
    DataCubeAPI,
    DuplicateKeyError,
    PaginationError,
    PaginationIncompleteError,
    PaginationProtocolError,
    PaginationReport,
    PartitionExecutionError,
    PartitionExecutionResult,
    PartitionCheckpointError,
    PartitionLockError,
    PartitionPlan,
    PartitionResult,
    TushareAPI,
)

__version__ = '0.1.9'
__all__ = [
    'TushareAPI',
    'APILimitDetector',
    'DataCubeAPI',
    'APIResponseError',
    'PaginationError',
    'PaginationProtocolError',
    'PaginationIncompleteError',
    'DuplicateKeyError',
    'PaginationReport',
    'PartitionPlan',
    'PartitionResult',
    'PartitionExecutionResult',
    'PartitionExecutionError',
    'PartitionCheckpointError',
    'PartitionLockError',
]
