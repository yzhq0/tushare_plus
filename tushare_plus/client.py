"""Tushare API 客户端

本模块提供了访问 Tushare 金融数据 API 的客户端实现。
主要功能：
1. 自动探测并记录各接口的单次传输限制和访问频率限制
2. 自动处理分页请求，支持获取超过单次传输限制的数据
3. 支持并发请求，提高大量数据获取效率
4. 实现访问频率控制，避免触发 API 调用限制
5. 错误处理和自动重试机制

使用示例：
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
        limit=240000
    )
"""

import json
import time
import logging
import os
import csv
from urllib.request import Request, urlopen
import pandas as pd
import concurrent.futures
from typing import Dict, Optional, Tuple

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('TushareAPI')

# DEFAULT_API_LIMITS_FILENAME = "api_limits.csv" # 不再需要全局默认，由各API类指定
CONFIG_DIR_NAME = ".tushare_plus"

class APILimitDetector:
    def __init__(self, csv_path: Optional[str] = None, default_filename: str = "api_limits.csv"):
        """初始化API限制参数检测器
        
        参数:
            csv_path: API限制参数CSV文件的路径。如果为None，则使用用户目录下的默认路径。
            default_filename: 当 csv_path 为 None 时，在用户目录下使用的默认文件名。
        """
        if csv_path is None:
            user_home = os.path.expanduser("~")
            config_dir = os.path.join(user_home, CONFIG_DIR_NAME)
            self.csv_path = os.path.join(config_dir, default_filename) # 使用传入的 default_filename
            os.makedirs(config_dir, exist_ok=True)
            logger.info(f"API限制参数文件将使用默认路径: {self.csv_path}")
        else:
            self.csv_path = csv_path
            dir_name = os.path.dirname(self.csv_path)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            logger.info(f"API限制参数文件将使用指定路径: {self.csv_path}")

        self._init_csv()
    
    def _init_csv(self):
        """初始化CSV文件"""
        if not os.path.exists(self.csv_path):
            # 创建CSV文件并写入表头
            with open(self.csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['api_name', 'limit_per_request', 'rate_limit', 'last_updated'])
    
    def get_api_limits(self, api_name: str) -> Optional[Dict]:
        """从CSV文件获取API限制参数"""
        if not os.path.exists(self.csv_path):
            return None
            
        try:
            df = pd.read_csv(self.csv_path)
            row = df[df['api_name'] == api_name]
            if not row.empty:
                # 确保返回的是Python原生类型，而不是NumPy类型
                return {
                    "limit_per_request": int(row['limit_per_request'].values[0]),
                    "rate_limit": int(row['rate_limit'].values[0]),
                    "last_updated": row['last_updated'].values[0]
                }
        except Exception as e:
            logger.warning(f"读取API限制参数失败: {str(e)}")
        
        return None
    
    def save_api_limits(self, api_name: str, limit_per_request: int, rate_limit: int):
        """保存API限制参数到CSV文件"""
        try:
            # 读取现有数据
            if os.path.exists(self.csv_path) and os.path.getsize(self.csv_path) > 0:
                df = pd.read_csv(self.csv_path)
                # 更新或添加记录
                if api_name in df['api_name'].values:
                    df.loc[df['api_name'] == api_name, 'limit_per_request'] = limit_per_request
                    df.loc[df['api_name'] == api_name, 'rate_limit'] = rate_limit
                    df.loc[df['api_name'] == api_name, 'last_updated'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                else:
                    new_row = pd.DataFrame({
                        'api_name': [api_name],
                        'limit_per_request': [limit_per_request],
                        'rate_limit': [rate_limit],
                        'last_updated': [pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')]
                    })
                    df = pd.concat([df, new_row], ignore_index=True)
            else:
                # 创建新的DataFrame
                df = pd.DataFrame({
                    'api_name': [api_name],
                    'limit_per_request': [limit_per_request],
                    'rate_limit': [rate_limit],
                    'last_updated': [pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')]
                })
            
            # 保存到CSV
            df.to_csv(self.csv_path, index=False)
            logger.info(f"API限制参数已保存到 {self.csv_path}")
        except Exception as e:
            logger.error(f"保存API限制参数失败: {str(e)}")

    def remove_api_limits(self, api_name: str):
        """从CSV文件删除指定API的限制参数"""
        if not os.path.exists(self.csv_path) or os.path.getsize(self.csv_path) == 0:
            logger.warning(f"API限制文件 {self.csv_path} 不存在或为空，无法删除 {api_name} 的限制。")
            return

        try:
            df = pd.read_csv(self.csv_path)
            if api_name in df['api_name'].values:
                df_filtered = df[df['api_name'] != api_name]
                # 如果过滤后DataFrame为空，to_csv会写入一个只有表头的文件
                df_filtered.to_csv(self.csv_path, index=False)
                logger.info(f"已从 {self.csv_path} 删除 {api_name} 的限制参数。")
            else:
                logger.info(f"在 {self.csv_path} 中未找到 {api_name} 的限制参数，无需删除。")
        except Exception as e:
            logger.error(f"从 {self.csv_path} 删除 {api_name} 的限制参数失败: {str(e)}")

class TushareAPI:

    def __init__(
        self,
        token=None,
        max_workers=5,
        max_retries=3,
        retry_delay=1,
        enable_rate_limit=True,
        custom_params_file=None,
        api_limits_file: Optional[str] = None,
        api_limits_default_filename: str = "tushare_api_limits.csv" # 新增参数，TushareAPI的默认文件名
    ):
        if token:
            self.token = token
        else:
            self.token = os.environ.get('TUSHARE_TOKEN')

        if not self.token:
            raise ValueError("Tushare token must be provided either as an argument or via TUSHARE_TOKEN environment variable.")

        self.api_url = "http://api.tushare.pro"
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        # APILimitDetector 会根据 api_limits_file 是否为 None 来决定路径
        # 如果 api_limits_file 为 None，则使用 api_limits_default_filename 在用户目录下创建文件
        self.limit_detector = APILimitDetector(
            csv_path=api_limits_file, 
            default_filename=api_limits_default_filename
        )
        self._api_last_call_time = {}
        self._api_info_cache = {}  # 添加缓存初始化
        self.enable_rate_limit = enable_rate_limit  # 添加频率限制开关

        # 加载API参数配置
        self._api_required_params = self._load_api_params(custom_params_file)

    def _load_api_params(self, custom_params_file=None):
        """加载API参数配置
        
        参数:
            custom_params_file: 自定义参数配置文件路径，如果为None则使用默认配置
        
        返回:
            API参数配置字典
        """
        # 默认参数配置
        default_params = {
            "index_weight": {"index_code": "000906.SH"}  # 基本配置，作为备用
        }

        # 尝试加载默认配置文件
        default_params_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'api_params.json')
        if os.path.exists(default_params_file):
            try:
                with open(default_params_file, 'r', encoding='utf-8') as f:
                    default_params = json.load(f)
                    logger.info(f"已加载默认API参数配置: {default_params_file}")
            except Exception as e:
                logger.warning(f"加载默认API参数配置失败: {str(e)}")

        # 如果提供了自定义配置文件，合并配置
        if custom_params_file and os.path.exists(custom_params_file):
            try:
                with open(custom_params_file, 'r', encoding='utf-8') as f:
                    custom_params = json.load(f)
                    # 合并配置，自定义配置优先
                    default_params.update(custom_params)
                    logger.info(f"已加载自定义API参数配置: {custom_params_file}")
            except Exception as e:
                logger.warning(f"加载自定义API参数配置失败: {str(e)}")

        return default_params

    def add_api_params(self, api_name, params):
        """添加或更新API参数
        
        参数:
            api_name: API接口名称
            params: 参数字典
        """
        self._api_required_params[api_name] = params
        logger.info(f"已添加API参数: {api_name} = {params}")

    def _detect_api_limits(self, api_name: str) -> Tuple[int, int]:
        """探测API的限制参数
        
        参数:
            api_name: API接口名称
        """
        logger.info(f"开始探测接口 {api_name} 的限制参数...")

        # 使用预定义的必要参数，不合并用户传入的参数
        required_params = self._api_required_params.get(api_name, {}).copy()

        # 首先探测单次请求限制
        limit = self._detect_request_limit(api_name, required_params)

        # 然后探测访问频率限制
        rate_limit = self._detect_rate_limit(api_name, required_params)

        # 保存探测结果
        self.limit_detector.save_api_limits(api_name, limit, rate_limit)
        logger.info(f"接口 {api_name} 的限制参数探测完成：单次限制 {limit}，频率限制 {rate_limit}/分钟")

        return limit, rate_limit

    def _detect_request_limit(self, api_name: str, required_params: Dict = None) -> int:
        """探测单次请求数据量限制
        
        参数:
            api_name: API接口名称
            required_params: 必要的请求参数
        """
        if required_params is None:
            required_params = {}

        try:
            logger.info(f"开始探测接口 {api_name} 的单次请求限制...")
            # 构造请求参数，包含必要参数
            params = required_params.copy()

            # 不设置limit参数，直接请求
            payload = {
                "api_name": api_name,
                "token": self.token,
                "params": params,
                "fields": ""
            }
            req = Request(
                self.api_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urlopen(req) as response:
                result = json.loads(response.read().decode("utf-8"))
                if result["code"] != 0:
                    raise Exception(f"Error {result['code']}: {result['msg']}")
                data = result["data"]
                count = len(data["items"])

                # 检查是否有has_more字段
                has_more = data.get("has_more", None)

                if has_more is not None:
                    # 如果API返回了has_more字段
                    if not has_more:
                        # has_more为False，说明这是所有数据，没有单次请求限制
                        logger.info(f"接口 {api_name} 可能没有单次请求限制，返回数据量为 {count} 条")
                        return 0  # 使用0表示没有限制，而不是float('inf')
                    else:
                        # has_more为True，说明有更多数据，当前返回量可能是单次限制
                        logger.info(f"接口 {api_name} 的单次请求限制为 {count} 条")
                        return count
                else:
                    # 如果API没有返回has_more字段，使用原来的判断逻辑
                    if count % 1000 == 0 and count > 0:
                        logger.info(f"接口 {api_name} 的单次请求限制为 {count} 条")
                        return count
                    else:
                        # 如果不是1000的整数倍，认为没有限制
                        logger.info(f"接口 {api_name} 可能没有单次请求限制，返回数据量为 {count} 条")
                        return 0  # 使用0表示没有限制，而不是float('inf')
        except Exception as e:
            logger.warning(f"探测接口 {api_name} 的单次请求限制失败: {str(e)}")
            # 失败时使用默认值
            return 5000

    def _detect_rate_limit(self, api_name: str, required_params: Dict = None) -> int:
        """探测每分钟访问频率限制
        
        参数:
            api_name: API接口名称
            required_params: 必要的请求参数
        """
        if required_params is None:
            required_params = {}

        # 使用小数据量快速测试
        test_limit = 100
        count = 0
        start_time = time.time()
        
        # 初始化该API的访问历史记录（用于后续频率控制）
        if not hasattr(self, '_api_call_history'):
            self._api_call_history = {}
        if api_name not in self._api_call_history:
            self._api_call_history[api_name] = []

        # 构造请求参数，包含必要参数
        params = required_params.copy()
        params["limit"] = test_limit

        # 避免循环调用，直接发送请求
        payload = {
            "api_name": api_name,
            "token": self.token,
            "params": params,
            "fields": ""
        }

        while time.time() - start_time < 60:
            try:
                req = Request(
                    self.api_url,
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST"
                )
                with urlopen(req) as response:
                    result = json.loads(response.read().decode("utf-8"))
                    if result["code"] != 0:
                        if "每分钟最多访问" in result["msg"]:
                            break
                        raise Exception(f"Error {result['code']}: {result['msg']}")
                count += 1
                # 记录本次调用时间到访问历史中
                self._api_call_history[api_name].append(time.time())
                # 短暂休息以避免立即触发限制
                # time.sleep(0.1)
            except Exception as e:
                if "每分钟最多访问" in str(e):
                    break
                else:
                    raise e


        detected_limit = max(1, count)
        
        # 如果探测过程中达到了限制，记录最后一次请求的时间
        if count > 0 and len(self._api_call_history[api_name]) > 0:
            logger.info(f"接口 {api_name} 在探测过程中发送了 {count} 次请求，可能需要等待API限制重置")
            
        return detected_limit

    def get_api_info(self, api_name: str) -> Dict:
        """获取API接口信息，如果没有则进行探测
        
        参数:
            api_name: API接口名称
        """
        # 尝试从缓存获取
        if api_name in self._api_info_cache:
            return self._api_info_cache[api_name]

        # 如果禁用了频率限制，使用0表示无限制
        if not self.enable_rate_limit:
            # 只探测单次请求限制，不探测频率限制
            cached_limits = self.limit_detector.get_api_limits(api_name)
            if cached_limits is None:
                # 没有缓存，只探测单次请求限制
                limit_per_request = self._detect_request_limit(api_name, self._api_required_params.get(api_name, {}))
                rate_limit = 0  # 使用0表示没有频率限制
                # 保存探测结果到CSV文件
                self.limit_detector.save_api_limits(api_name, limit_per_request, rate_limit)
            else:
                limit_per_request = int(cached_limits["limit_per_request"]) if cached_limits["limit_per_request"] != 0 else 0
                rate_limit = 0  # 使用0表示没有频率限制
        else:
            # 尝试从CSV文件获取缓存的限制参数
            cached_limits = self.limit_detector.get_api_limits(api_name)

            if cached_limits is None:
                # 没有缓存，进行探测
                limit_per_request, rate_limit = self._detect_api_limits(api_name)
                
                # 探测完成后，检查是否需要等待API限制重置
                # 确保在探测后有足够的时间间隔再进行实际数据请求
                if hasattr(self, '_api_call_history') and api_name in self._api_call_history and len(self._api_call_history[api_name]) > 0:
                    self._respect_rate_limit(api_name)
            else:
                # 确保是Python原生类型
                limit_per_request = int(cached_limits["limit_per_request"])
                rate_limit = int(cached_limits["rate_limit"])

        # 保存到缓存
        info = {
            "limit_per_request": limit_per_request,
            "rate_limit": rate_limit
        }
        self._api_info_cache[api_name] = info
        return info

    def clear_api_limits(self, api_name: str):
        """清除指定API的限制参数（内存缓存和CSV文件）"""
        logger.info(f"开始清除接口 {api_name} 的限制参数...")

        # 从CSV文件清除
        self.limit_detector.remove_api_limits(api_name)

        # 从内存缓存清除
        if api_name in self._api_info_cache:
            del self._api_info_cache[api_name]
            logger.info(f"已从缓存中清除接口 {api_name} 的限制参数。")
        else:
            logger.info(f"接口 {api_name} 的限制参数未在内存缓存中找到。")

    def force_redetect_api_limits(self, api_name: str):
        """
        强制清除并重新探测指定API的限制参数。
        此方法会首先清除该API在内存缓存和CSV文件中的现有记录，
        然后立即触发新的限制参数探测过程。
        """

        # 1. 清除现有的限制参数 (包括内存缓存和CSV文件中的记录)
        # clear_api_limits 方法内部会处理详细的日志记录
        self.clear_api_limits(api_name)

        # 2. 重新获取API信息，这将触发探测逻辑（因为缓存已被清除）
        # get_api_info 方法会负责探测、保存到CSV并更新内存缓存
        logger.info(f"缓存清除完毕，开始为接口 {api_name} 重新进行参数探测。")
        try:
            # 调用 get_api_info 会触发探测（如果需要）并返回更新后的信息
            new_limits = self.get_api_info(api_name)

        except Exception as e:
            logger.error(f"在为接口 {api_name} 强制重新探测参数时发生错误: {e}")
            # 即使探测失败，之前的清除操作也已完成
            logger.info(f"接口 {api_name} 的旧有参数已被清除，但新的探测未能成功。请检查错误信息。")

    def _make_request(self, api_name, params, fields, retry_count=0):
        """构造并发送HTTP POST请求，支持重试机制"""
        # 检查并遵守访问频率限制
        # 避免循环调用，只在非探测模式下检查频率限制
        if self.enable_rate_limit and api_name in self._api_info_cache:
            self._respect_rate_limit(api_name)

        payload = {
            "api_name": api_name,
            "token": self.token,
            "params": params,
            "fields": fields
        }
        req = Request(
            self.api_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        try:
            with urlopen(req) as response:
                result = json.loads(response.read().decode("utf-8"))
                if result["code"] != 0:
                    # 记录错误并根据错误类型定义是否重试
                    error_msg = f"Error {result['code']}: {result['msg']}"
                    if retry_count < self.max_retries and self._should_retry(result["code"]):
                        logger.warning(f"{api_name} 请求失败，将在 {self.retry_delay} 秒后重试: {error_msg}")
                        time.sleep(self.retry_delay)
                        return self._make_request(api_name, params, fields, retry_count + 1)
                    raise Exception(error_msg)
                return result["data"]
        except Exception as e:
            if retry_count < self.max_retries:
                logger.warning(f"{api_name} 请求失败，将在 {self.retry_delay} 秒后重试: {str(e)}")
                time.sleep(self.retry_delay)
                return self._make_request(api_name, params, fields, retry_count + 1)
            raise Exception(f"Request failed after {self.max_retries} retries: {str(e)}")

    def _should_retry(self, error_code):
        """根据错误码判断是否应该重试"""
        # 可以根据 API 文档中的错误码定义来完善此函数
        # 通常，网络错误、服务器临时错误应该重试，参数错误等不应重试
        retry_error_codes = [
            -1,  # 系统错误
            40203,  # 请求过于频繁
            500,  # 服务器内部错误
            503   # 服务不可用
        ]
        return error_code in retry_error_codes

    def _respect_rate_limit(self, api_name):
        """遵守 API 访问频率限制
        
        使用滑动窗口方式实现频率控制，确保在任意 60 秒内的请求次数不超过限制
        """
        # 获取接口的访问频率限制
        api_info = self._api_info_cache.get(api_name, {"rate_limit": 60})
        rate_limit = api_info.get('rate_limit', 60)  # 默认每分钟 60 次

        # 如果rate_limit为0，表示没有频率限制，直接返回
        if rate_limit == 0:
            return

        # 初始化该 API 的访问历史记录
        if not hasattr(self, '_api_call_history'):
            self._api_call_history = {}

        if api_name not in self._api_call_history:
            self._api_call_history[api_name] = []

        # 获取当前时间
        now = time.time()

        # 清理超过 60 秒的历史记录
        self._api_call_history[api_name] = [t for t in self._api_call_history[api_name] 
                                           if now - t < 60]

        # 检查当前窗口内的请求数量
        if len(self._api_call_history[api_name]) >= rate_limit:
            # 计算需要等待的时间
            oldest_call = min(self._api_call_history[api_name])
            wait_time = 60 - (now - oldest_call)

            if wait_time > 0:
                logger.debug(f"等待 {wait_time:.2f} 秒以遵守 {api_name} 的访问频率限制")
                time.sleep(wait_time)
                # 更新当前时间
                now = time.time()

        # 记录本次调用时间
        self._api_call_history[api_name].append(now)

    def get_data(self, api_name, fields="", auto_paging=True, concurrent=False, max_pages=None, **params):
        """
        获取接口数据并返回DataFrame
        
        参数:
            api_name: API接口名称
            fields: 需要获取的字段，逗号分隔的字符串
            auto_paging: 是否自动处理分页
            concurrent: 是否使用并发请求
            max_pages: 最大分页数量，用于并发模式下控制请求数量
            **params: API的其他参数
        
        返回:
            包含请求数据的DataFrame
        """
        # 如果不需要自动分页，直接调用原始方法
        if not auto_paging:
            data = self._make_request(api_name, params, fields)
            return pd.DataFrame(data["items"], columns=data["fields"])

        # 获取接口的单次传输限制
        api_info = self.get_api_info(api_name)
        limit_per_request = api_info.get('limit_per_request', 5000)

        # 如果接口没有单次查询上限（值为0），直接请求
        if limit_per_request == 0:
            data = self._make_request(api_name, params, fields)
            return pd.DataFrame(data["items"], columns=data["fields"])

        # 设置分页参数
        offset = params.get('offset', 0)

        # 用户可能指定了limit参数
        user_limit = params.get('limit', None)

        # 如果是并发模式，需要预先确定页数
        if concurrent:
            if max_pages is None:
                # 如果用户指定了limit，计算需要的页数
                if user_limit is not None:
                    max_pages = (user_limit + limit_per_request - 1) // limit_per_request
                else:
                    # 默认尝试10页，用户可以通过max_pages参数调整
                    max_pages = 1000
                    logger.warning(f"并发模式下未指定max_pages或limit，默认尝试获取{max_pages}页数据")

            # 准备分页参数
            page_params = []
            for page in range(max_pages):
                page_offset = offset + page * limit_per_request

                # 如果用户指定了limit，确保不超过用户指定的总量
                if user_limit is not None:
                    remaining = user_limit - page * limit_per_request
                    if remaining <= 0:
                        break
                    page_limit = min(limit_per_request, remaining)
                else:
                    page_limit = limit_per_request

                page_param = params.copy()
                page_param['offset'] = page_offset
                page_param['limit'] = page_limit
                page_params.append((api_name, page_param, fields))

            # 使用并发请求
            return self._get_data_concurrent(page_params)
        else:
            # 顺序模式，循环获取所有数据
            all_data = []
            fields_list = None
            total_fetched = 0

            while True:
                # 复制参数，设置当前页的offset和limit
                page_params = params.copy()
                page_params['offset'] = offset

                # 如果用户指定了limit，确保不超过用户指定的总量
                if user_limit is not None:
                    remaining = user_limit - total_fetched
                    if remaining <= 0:
                        break
                    page_params['limit'] = min(limit_per_request, remaining)
                else:
                    page_params['limit'] = limit_per_request

                # 请求当前页数据
                logger.info(f"请求 {api_name} 数据: offset={offset}, limit={page_params['limit']}")
                data = self._make_request(api_name, page_params, fields)

                # 保存字段名
                if fields_list is None:
                    fields_list = data["fields"]

                # 获取当前页数据条数
                current_count = len(data["items"])

                # 添加到结果集
                all_data.extend(data["items"])
                total_fetched += current_count

                # 使用has_more字段判断是否还有更多数据
                has_more = data.get("has_more", False)
                if not has_more:
                    # API明确表示没有更多数据
                    break

                # 更新offset，准备获取下一页
                offset += current_count

                # 如果用户指定了limit并且已经达到，停止获取
                if user_limit is not None and total_fetched >= user_limit:
                    break

            logger.info(f"共获取 {len(all_data)} 条 {api_name} 数据")
            return pd.DataFrame(all_data, columns=fields_list)

    def _get_data_concurrent(self, page_params):
        """并发请求多页数据"""
        all_data = []
        fields = None

        def fetch_page(params_tuple):
            api_name, params, field_str = params_tuple
            logger.info(f"并发请求 {api_name} 数据: offset={params.get('offset', 0)}, limit={params.get('limit', 0)}")
            try:
                return self._make_request(api_name, params, field_str)
            except Exception as e:
                # 如果是因为偏移量超过了实际数据量，返回空结果
                if "offset" in str(e).lower() or "超出范围" in str(e):
                    logger.warning(f"偏移量可能超出范围: {str(e)}")
                    return {"fields": field_str.split(",") if field_str else [], "items": [], "has_more": False}
                raise

        # 按照offset排序，确保从小到大处理
        sorted_params = sorted(page_params, key=lambda x: x[1].get('offset', 0))

        # 记录连续空结果的数量
        empty_results_count = 0
        max_empty_results = 2 # 连续两页空结果就认为没有更多数据

        # 分批提交任务，而不是一次性提交所有任务
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            batch_size = self.max_workers  # 每批提交的任务数量

            for i in range(0, len(sorted_params), batch_size):
                # 如果已经连续获取到多个空结果，提前终止
                if empty_results_count >= max_empty_results:
                    logger.info(f"连续 {max_empty_results} 页数据为空，提前终止请求")
                    break

                # 获取当前批次的参数
                batch_params = sorted_params[i:i+batch_size]

                # 提交当前批次的任务
                future_to_params = {executor.submit(fetch_page, param): param for param in batch_params}

                # 处理当前批次的结果
                for future in concurrent.futures.as_completed(future_to_params):
                    try:
                        data = future.result()
                        if fields is None and data["fields"]:
                            fields = data["fields"]

                        # 检查结果是否为空
                        if not data["items"]:
                            empty_results_count += 1
                        else:
                            empty_results_count = 0  # 重置计数器
                            all_data.extend(data["items"])

                        # 检查是否还有更多数据
                        has_more = data.get("has_more", None)
                        if has_more is not None and not has_more:
                            # API明确表示没有更多数据
                            empty_results_count = max_empty_results  # 强制提前终止
                    except Exception as e:
                        param = future_to_params[future]
                        logger.error(f"请求失败 {param[0]}: {str(e)}")
                        raise

        # 如果没有获取到任何数据，返回空DataFrame
        if not fields:
            return pd.DataFrame()


class DataCubeAPI(TushareAPI):
    """
    用于访问类似Tushare的数据源的客户端，方正DataCube。
    默认禁用API访问频率限制。
    """
    def __init__(
        self,
        token=None,
        max_workers=5,
        max_retries=3,
        retry_delay=1,
        custom_params_file=None,
        api_limits_file: Optional[str] = None,
        api_limits_default_filename: str = "datacube_api_limits.csv"
    ):

        if not token:
            token = os.environ.get('DATACUBE_TOKEN')

        if not token:
            raise ValueError("Token must be provided either as an argument or via DATACUBE_TOKEN environment variable.")

        # 调用父类的构造函数，并明确设置 enable_rate_limit=False
        super().__init__(
            token=token,
            max_workers=max_workers,
            max_retries=max_retries,
            retry_delay=retry_delay,
            enable_rate_limit=False,  # 禁用频率限制
            custom_params_file=custom_params_file,
            api_limits_file=api_limits_file,
            api_limits_default_filename=api_limits_default_filename
        )
        # 设置新的API URL
        self.api_url = "http://datacubeapi.foundersc.com"
        self._api_required_params['fund_nav'] = {'end_date': '20250506'}  # 方正DataCube的必要参数
