"""
东方财富 HTTP 基础客户端

特性：
- 自动携带浏览器 Headers + Referer
- 支持重试（默认 3 次，指数退避）
- 代理可选配置
"""
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://quote.eastmoney.com/",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

_PUSH2_BASE     = "https://push2.eastmoney.com/api/qt"
_PUSH2HIS_BASE  = "https://push2his.eastmoney.com/api/qt"
_DATACENTER_BASE = "https://datacenter-web.eastmoney.com/api/data/v1"


class EastMoneyClient:
    """
    东方财富接口通用 HTTP 客户端

    Parameters
    ----------
    proxies : dict | None
        代理配置，例如 {'https': 'http://127.0.0.1:7890'}。
        None 表示跟随系统代理；{} 表示禁用代理。
    timeout : float
        单次请求超时（秒）
    max_retries : int
        请求失败时的最大重试次数
    retry_backoff : float
        重试退避系数（指数退避）
    """

    def __init__(
        self,
        proxies: dict | None = None,
        timeout: float = 15.0,
        max_retries: int = 3,
        retry_backoff: float = 0.5,
    ):
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(_DEFAULT_HEADERS)

        if proxies is not None:
            self._session.proxies.update(proxies)

        retry = Retry(
            total=max_retries,
            backoff_factor=retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    # ------------------------------------------------------------------
    # 内部方法
    # ------------------------------------------------------------------

    def _get(self, url: str, params: dict | None = None) -> dict:
        """发起 GET 请求并返回解析后的 JSON"""
        resp = self._session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # push2 接口（实时行情 / 成分股 / 资金流）
    # ------------------------------------------------------------------

    def clist_get(self, params: dict) -> dict:
        """
        调用 push2 clist 接口（板块列表 / 成分股列表）

        Returns raw JSON response dict.
        """
        url = f"{_PUSH2_BASE}/clist/get"
        return self._get(url, params)

    # ------------------------------------------------------------------
    # push2his 接口（历史 K 线）
    # ------------------------------------------------------------------

    def kline_get(self, params: dict) -> dict:
        """调用 push2his kline 接口"""
        url = f"{_PUSH2HIS_BASE}/stock/kline/get"
        return self._get(url, params)
