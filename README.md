# Tushare Plus

增强版Tushare API客户端，提供自动分页、并发请求和频率限制功能。

## 特点

- **自动探测限制**：自动探测并记录各接口的单次传输限制和访问频率限制
- **自动分页**：自动处理分页请求，支持获取超过单次传输限制的数据
- **并发请求**：支持并发请求，提高大量数据获取效率
- **频率控制**：实现访问频率控制，避免触发API调用限制
- **错误处理**：内置错误处理和自动重试机制
- **完整性审计**：严格分页终止、跨页schema/主键检查和逐页报告
- **可恢复分区**：原子文件、哈希sidecar、执行manifest和跨进程排他锁

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

项目继续支持 Python 3.6。构建依赖会在 Python 3.6 使用兼容的
`setuptools<60`/`wheel<0.38`，较新Python使用现代setuptools。当前代码会做
Python 3.6语法门禁；发布前仍建议在真实Python 3.6环境运行完整测试矩阵。

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

首次使用某个接口时，建议保留默认探测逻辑。DataCube 或 Tushare 页面上的限制参数可能滞后，运行时探测结果更可靠。

如果接口已经通过历史运行或本地缓存验证过分页大小，可以显式传入 `limit_per_request`，避免重复探测带来的额外耗时。`detect_limit=False` 只适合已验证接口的复跑；未验证接口的大批量生产不要直接跳过探测。

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
    detect_limit=False,  # 已验证接口复跑时，使用默认分页大小5000，不触发自动探测
)
```

### 返回类型

`get_data` 默认返回 `pandas.DataFrame`。如果下游计算使用 Polars、Arrow，或需要保留原始 API 结构，可以通过 `return_type` 切换。

```python
df = client.get_data("daily", fields="ts_code,trade_date,close")

raw = client.get_data(
    "daily",
    fields="ts_code,trade_date,close",
    return_type="raw",  # {"fields": [...], "items": [[...], ...]}
)

polars_df = client.get_data(
    "daily",
    fields="ts_code,trade_date,close",
    return_type="polars",  # 需要安装 polars
)

arrow_table = client.get_data(
    "daily",
    fields="ts_code,trade_date,close",
    return_type="arrow",  # 需要安装 pyarrow
)
```

### 严格分页与逐页报告

`get_data` 的默认返回值保持不变。设置 `return_report=True` 后返回
`(data, PaginationReport)`；声明 `primary_key` 可在服务端offset分页发生重叠时立即失败。

```python
from tushare_plus import PaginationIncompleteError, PaginationProtocolError

df, report = client.get_data(
    "daily",
    fields="ts_code,trade_date,close",
    start_date="20260101",
    end_date="20260131",
    limit_per_request=5000,
    max_pages=100,
    primary_key=("ts_code", "trade_date"),
    strict_paging=True,
    return_report=True,
)

print(report.request_satisfied, report.source_exhausted)
print(report.pages)  # 每页offset、请求量、返回量、has_more和可选首末主键
```

严格模式会拒绝以下情况：达到 `max_pages` 但仍有更多数据、空页却
`has_more=True`、跨页字段变化、并发页被服务端静默缩短，以及同一并发批次中
较低页声称取尽但更高页仍返回数据。顺序和并发模式都会拒绝在新offset重复出现的
相同非空页，以防服务端忽略offset后产生无限循环或把重复行误计为显式limit。
无 `has_more` 的旧接口在顺序模式下会继续请求到
空页，并以 `exhaustion_inferred=True` 记录“推断取尽”；并发模式不会用非空短页推断安全offset。

`auto_paging=False` 明确表示只执行一次请求。无显式 `limit` 时，单次调用成功即
`request_satisfied=True`，即使服务端仍有更多数据；若指定 `limit=N`，严格模式会拒绝
返回超过N行，或返回少于N行却同时声明 `has_more=True` 的矛盾响应。

`complete`/`request_satisfied` 表示调用方声明的请求已经完成，例如达到显式 `limit`；
这不等于整个数据源已经取尽。`source_exhausted` 只记录服务端明确的取尽声明。
业务数据集是否完整仍需调用方根据预期主键、日期或截面基数验证。

### 可恢复的分块下载

`PartitionPlan`/`execute_partition_plan` 是生产长任务的执行接口。调用方负责按业务场景
构造日期块、代码块或其他参数块；库负责路径预检、并发执行、原子落盘和可恢复审计。
计划在构造时深拷贝参数并冻结配置，执行期间使用同一份完整快照。

```python
from tushare_plus import PartitionPlan

chunks = [
    {"trade_date": "20260105"},
    {"trade_date": "20260106"},
]

plan = PartitionPlan(
    api_name="daily",
    param_chunks=chunks,
    output_dir="output/daily",
    fields="ts_code,trade_date,close,vol",
    file_format="parquet",
    limit_per_request=5000,
    primary_key=("ts_code", "trade_date"),
    partition_workers=4,
)

result = client.execute_partition_plan(
    plan,
    resume=True,
    continue_on_error=False,
)
print(result.complete, result.written, result.resumed, result.manifest_path)
```

每个数据文件都有 `<filename>.meta.json` sidecar，记录无凭据的source identity、请求指纹、
计划指纹、行数、SHA-256和分页报告。请求指纹绑定client class、完整API URL的SHA-256
及domain-separated认证身份哈希；展示的origin不含userinfo、path或query，token不落明文。
轮换token会使旧checkpoint不再匹配并要求重新下载。`resume=True` 还会交叉验证脱敏参数、
`row_count`与分页`rows_fetched`，只接受合同和哈希均匹配的checkpoint：孤立文件、
不可解析sidecar或指纹不匹配会失败关闭；合同匹配但数据哈希损坏时会原子重拉并在
manifest标记 `invalid_checkpoint_replaced`。使用 `resume=False` 才会显式覆盖不匹配产物。

执行manifest列出所有分区的 `written`、`resumed`、`failed` 或 `not_run` 状态。
数据、sidecar和manifest都通过同目录临时文件原子替换并严格fsync文件；支持目录fsync的
平台还会同步目录项，不支持的平台会安全降级。output及外部manifest使用
跨进程排他锁，冲突执行会立即失败并提示检查stale lock。token、password、secret、
api_key等参数会从默认文件名、sidecar、manifest和错误文本中递归脱敏；敏感主键的
首末值只记录为`***REDACTED***`，不会改变原始DataFrame或数据文件。

兼容接口 `download_partitions` 仍返回路径列表，但内部使用同一执行引擎：

```python
paths = client.download_partitions(
    "daily",
    chunks,
    "output/daily_csv",
    fields="ts_code,trade_date,close,vol",
    limit_per_request=5000,
    primary_key=("ts_code", "trade_date"),
    partition_workers=4,
)
```

`iter_data` 保留为不落盘的轻量流式原语：

```python
for params, df in client.iter_data(
    "daily",
    chunks,
    fields="ts_code,trade_date,close,vol",
    limit_per_request=5000,
):
    print(params, len(df))
```

## 与官方SDK的区别

相比官方的Tushare SDK，Tushare Plus提供了以下增强功能：

1. 自动处理分页，无需手动编写循环代码
2. 支持并发请求，大幅提高数据获取效率
3. 智能频率控制，避免触发API限制
4. 自动探测各接口的限制参数
5. 更完善的错误处理和重试机制
6. 可审计、可恢复且失败关闭的分页与分区执行

## 已知问题

客户端只能验证服务端实际返回的页。若服务端在不产生重复键的情况下遗漏未知记录，
客户端无法凭 `has_more` 或总行数还原缺失键；生产任务必须提供业务侧expected-key或
截面基数合同。重复页签名采用失败关闭策略；如果两个不同offset的合法页内容完全相同，
且调用方未提供可区分的主键，也会被保守地判为协议错误。另有以下已知问题：

1. 错误代码处理不完善，当前实现的错误代码与Tushare实际的错误代码可能不一致

## 许可证

MIT
