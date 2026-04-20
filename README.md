# Tushare Plus

增强版Tushare API客户端，提供自动分页、并发请求和频率限制功能。

## 特点

- **自动探测限制**：自动探测并记录各接口的单次传输限制和访问频率限制
- **自动分页**：自动处理分页请求，支持获取超过单次传输限制的数据
- **并发请求**：支持并发请求，提高大量数据获取效率
- **频率控制**：实现访问频率控制，避免触发API调用限制
- **错误处理**：内置错误处理和自动重试机制

## 安装

### 从源代码安装

```bash
# 克隆仓库
git clone https://github.com/yzhq0/tushare_plus.git
cd tushare_plus

# 安装
pip install -e .
```

或者直接从源代码安装：

```bash
pip install git+https://github.com/yzhq0/tushare_plus.git
```

## 快速开始

```python
from tushare_plus import TushareAPI

# 初始化客户端
client = TushareAPI(token="your_token_here")

# 获取股票基本信息
df = client.get_data(
    api_name="stock_basic",
    fields="ts_code,name,industry,area",
    list_status="L"
)

# 获取大量日线数据（自动处理分页）
df_daily = client.get_data(
    api_name="daily",
    fields="ts_code,trade_date,open,high,low,close,vol",
    limit=240000  # 自动处理分页
)

# 使用并发模式获取数据
df_concurrent = client.get_data(
    api_name="daily",
    fields="ts_code,trade_date,open,high,low,close,vol",
    concurrent=True,  # 启用并发模式
    limit=240000
)
```

## 高级用法

### 禁用频率限制

```python
# 在本地测试环境中可以禁用频率限制
client = TushareAPI(token="your_token_here", enable_rate_limit=False)
```

### 自定义并发数量

```python
# 设置最大并发请求数
client = TushareAPI(token="your_token_here", max_workers=10)
```

### 自定义重试策略

```python
# 设置最大重试次数和重试延迟
client = TushareAPI(token="your_token_here", max_retries=5, retry_delay=2)
```

### 长任务稳定性

```python
client = TushareAPI(
    token="your_token_here",
    request_timeout=60,  # 单次HTTP请求超时，None表示不设置
    max_retries=5,
    retry_delay=1,
    retry_backoff=2.0,   # 指数退避
    retry_jitter=0.1,    # 随机抖动，降低并发重试同步冲击
)
```

### 跳过或覆盖限制探测

大表生产任务如果已经知道分页大小，可以显式传入 `limit_per_request`，避免首次无界探测带来的额外耗时。

```python
df = client.get_data(
    api_name="daily",
    fields="ts_code,trade_date,open,high,low,close,vol",
    start_date="20260101",
    end_date="20260131",
    limit_per_request=5000,
)

df = client.get_data(
    api_name="daily",
    fields="ts_code,trade_date,open,high,low,close,vol",
    start_date="20260101",
    end_date="20260131",
    detect_limit=False,  # 使用默认分页大小5000，不触发自动探测
)
```

### 通用分块下载

`iter_data` 和 `download_partitions` 只提供通用执行原语，不内置任何接口或业务profile。调用方负责按业务场景构造日期块、代码块或其他参数块。

```python
chunks = [
    {"trade_date": "20260105"},
    {"trade_date": "20260106"},
]

for params, df in client.iter_data(
    "daily",
    chunks,
    fields="ts_code,trade_date,close,vol",
    limit_per_request=5000,
):
    print(params, len(df))

paths = client.download_partitions(
    "daily",
    chunks,
    "output/daily",
    fields="ts_code,trade_date,close,vol",
    limit_per_request=5000,
)
```

## 与官方SDK的区别

相比官方的Tushare SDK，Tushare Plus提供了以下增强功能：

1. 自动处理分页，无需手动编写循环代码
2. 支持并发请求，大幅提高数据获取效率
3. 智能频率控制，避免触发API限制
4. 自动探测各接口的限制参数
5. 更完善的错误处理和重试机制

## 已知问题

当前版本存在以下已知问题，将在后续版本中改进：

1. 错误代码处理不完善，当前实现的错误代码与Tushare实际的错误代码可能不一致

## 许可证

MIT
