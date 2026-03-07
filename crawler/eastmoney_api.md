# 东方财富接口完整文档

数据来源：EastMoney push2 / push2his
覆盖：板块行情 / 板块成分股 / 板块资金流 / K线 / 个股行情 / 个股资金流

---

## 目录

1. [基础信息](#1-基础信息)
2. [push2 clist 接口](#2-push2-clist-接口)
   - 2.1 板块列表（行业 / 概念 / 地域）
   - 2.2 板块成分股列表
   - 2.3 板块资金流向
   - 2.4 个股实时行情
   - 2.5 个股资金流向
3. [push2his kline 接口](#3-push2his-kline-接口)
4. [参数完整说明](#4-参数完整说明)
5. [字段完整枚举](#5-字段完整枚举)
6. [板块代码表](#6-板块代码表)
7. [Python SDK 用法](#7-python-sdk-用法)
8. [数据文件结构](#8-数据文件结构)
9. [维护脚本](#9-维护脚本)

---

## 1 基础信息

### Base URL

| 域名 | 用途 |
|------|------|
| `https://push2.eastmoney.com/api/qt` | 实时行情、成分股、资金流（clist） |
| `https://push2his.eastmoney.com/api/qt` | 历史 K 线 |

### 通用请求 Headers

```
User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36
Referer: https://quote.eastmoney.com/
Accept-Language: zh-CN,zh;q=0.9
```

### 通用响应结构

```json
{
  "rc": 0,
  "data": {
    "total": 497,
    "diff": {
      "0": { "f12": "BK0420", "f14": "航空", "f2": 1234.56, ... },
      "1": { ... }
    }
  }
}
```

- `data.total`：总条数
- `data.diff`：数据字典，key 为序号字符串
- 字段值为 `"-"` 表示无数据，解析时应转换为 `null`

---

## 2 push2 clist 接口

**接口地址**：`https://push2.eastmoney.com/api/qt/clist/get`

**通用参数**

| 参数 | 类型 | 含义 |
|------|------|------|
| `pn` | int | 页码（从 1 开始） |
| `pz` | int | 每页条数（最大约 500） |
| `fs` | str | 市场/板块筛选（见 [4.1 fs 参数](#41-fs-参数)） |
| `fields` | str | 返回字段，逗号分隔（见 [5 字段枚举](#5-字段完整枚举)） |
| `fid` | str | 排序字段（如 `f3` 按涨跌幅，`f62` 按主力净流入） |
| `po` | int | 排序方向：`1` 降序，`0` 升序 |

---

### 2.1 板块列表（行业 / 概念 / 地域）

**示例**

```
# 行业板块（按涨跌幅降序）
GET https://push2.eastmoney.com/api/qt/clist/get
  ?pn=1&pz=200&fs=m:90+t:2&fields=f12,f14,f2,f3,f4,f5,f6,f7,f8,f20,f21&fid=f3&po=1

# 概念板块
GET .../clist/get?fs=m:90+t:3&fields=f12,f14,f3,f6

# 地域板块
GET .../clist/get?fs=m:90+t:1&fields=f12,f14,f3
```

**返回字段（板块行情默认集）**

| 字段 | 含义 | 单位 |
|------|------|------|
| `f12` | 板块代码 | — |
| `f14` | 板块名称 | — |
| `f2` | 最新价 | — |
| `f3` | 涨跌幅 | % |
| `f4` | 涨跌额 | — |
| `f5` | 成交量 | 手 |
| `f6` | 成交额 | 元 |
| `f7` | 振幅 | % |
| `f8` | 换手率 | % |
| `f20` | 总市值 | 元 |
| `f21` | 流通市值 | 元 |

---

### 2.2 板块成分股列表

**fs 格式**：`b:{BK代码}`

**示例**

```
# 新能源车（BK0428）成分股，按涨跌幅降序
GET .../clist/get?pn=1&pz=500&fs=b:BK0428
  &fields=f12,f14,f2,f3,f4,f5,f6,f7,f8,f9,f10,f15,f16,f17,f18&fid=f3&po=1
```

**返回字段（成分股默认集）**

| 字段 | 含义 | 单位 |
|------|------|------|
| `f12` | 股票代码 | — |
| `f14` | 股票名称 | — |
| `f2` | 最新价 | — |
| `f3` | 涨跌幅 | % |
| `f4` | 涨跌额 | — |
| `f5` | 成交量 | 手 |
| `f6` | 成交额 | 元 |
| `f7` | 振幅 | % |
| `f8` | 换手率 | % |
| `f9` | 市盈率 | — |
| `f10` | 量比 | — |
| `f15` | 最高价 | — |
| `f16` | 最低价 | — |
| `f17` | 开盘价 | — |
| `f18` | 昨收价 | — |

---

### 2.3 板块资金流向

**fs 格式**：与板块列表相同（`m:90+t:2` / `m:90+t:3` / `m:90+t:1`）

**示例**

```
# 行业板块资金流，按主力净流入降序
GET .../clist/get?pn=1&pz=200&fs=m:90+t:2
  &fields=f12,f14,f62,f66,f69,f72,f75,f78,f81&fid=f62&po=1
```

**资金流字段**

| 字段 | 含义 | 单位 |
|------|------|------|
| `f62` | 主力净流入（超大单+大单净流入之和） | 元 |
| `f66` | 超大单流入 | 元 |
| `f69` | 超大单流出 | 元 |
| `f72` | 大单流入 | 元 |
| `f75` | 大单流出 | 元 |
| `f78` | 中单流入 | 元 |
| `f81` | 小单流入 | 元 |

> 注：板块资金流接口不返回中单流出（f84）和小单流出（f87），个股资金流接口才有。

---

### 2.4 个股实时行情

支持三种 `fs` 模式：

| 模式 | fs 格式 | 说明 |
|------|---------|------|
| 按板块成分 | `b:BK0428` | 某板块全部成分股 |
| 按指定代码 | `s:1600519,s:0000001` | 指定股票列表 |
| 全市场 A 股 | `m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048` | 沪深主板+创业板 |

**示例**

```
# DeepSeek 板块（BK0581）成分股行情
GET .../clist/get?pn=1&pz=500&fs=b:BK0581
  &fields=f12,f14,f2,f3,f4,f5,f6,f7,f8,f9,f10,f15,f16,f17,f18,f20,f21
  &fid=f3&po=1

# 指定股票：贵州茅台 + 平安银行
GET .../clist/get?pn=1&pz=10&fs=s:1600519,s:0000001
  &fields=f12,f14,f2,f3,f6
```

**个股代码 fs 前缀规则**

| 前缀 | 市场 | 示例 |
|------|------|------|
| `s:1{code}` | 沪市（6 开头） | `s:1600519` |
| `s:0{code}` | 深市/创业板/科创板 | `s:0000001` |

**返回字段**：与 [2.2 成分股](#22-板块成分股列表) 相同，可额外加 `f20`（总市值）、`f21`（流通市值）

---

### 2.5 个股资金流向

**示例**

```
# 人形机器人板块（BK0550）成分股资金流，按主力净流入降序
GET .../clist/get?pn=1&pz=500&fs=b:BK0550
  &fields=f12,f14,f62,f66,f69,f72,f75,f78,f84,f81,f87&fid=f62&po=1
```

**个股资金流字段（比板块多中单流出、小单流出）**

| 字段 | 含义 | 单位 |
|------|------|------|
| `f62` | 主力净流入 | 元 |
| `f66` | 超大单流入 | 元 |
| `f69` | 超大单流出 | 元 |
| `f72` | 大单流入 | 元 |
| `f75` | 大单流出 | 元 |
| `f78` | 中单流入 | 元 |
| `f84` | 中单流出 | 元 |
| `f81` | 小单流入 | 元 |
| `f87` | 小单流出 | 元 |

---

## 3 push2his kline 接口

**接口地址**：`https://push2his.eastmoney.com/api/qt/stock/kline/get`

**示例**

```
# 人工智能板块（BK0475）日K线，前复权，最近 500 条
GET .../stock/kline/get?secid=90.BK0475&klt=101&fqt=1&lmt=500
  &fields1=f1,f2,f3,f4,f5,f6
  &fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61

# 贵州茅台（600519）日K线，指定日期范围
GET .../stock/kline/get?secid=1.600519&klt=101&fqt=1&beg=20230101&end=20231231
  &fields1=f1,f2,f3,f4,f5,f6
  &fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61
```

**请求参数**

| 参数 | 含义 | 示例 |
|------|------|------|
| `secid` | 证券 ID，格式 `{市场}.{代码}` | `90.BK0475` / `1.600519` / `0.000001` |
| `klt` | K 线周期（见下表） | `101` |
| `fqt` | 复权类型（见下表） | `1` |
| `lmt` | 最大返回条数 | `500` |
| `beg` | 开始日期 `YYYYMMDD`，缺省不限 | `20230101` |
| `end` | 结束日期 `YYYYMMDD`，缺省不限 | `20231231` |
| `fields1` | 基础字段（固定为 `f1,f2,f3,f4,f5,f6`） | — |
| `fields2` | K 线列字段（固定为 `f51,...,f61`） | — |

**secid 市场前缀**

| 前缀 | 市场 |
|------|------|
| `90` | 板块（BK 代码） |
| `1` | 沪市 A 股（6 开头） |
| `0` | 深市 / 创业板（0、3 开头） |

**klt K线周期**

| 值 | 周期 |
|----|------|
| `1` | 1 分钟 |
| `5` | 5 分钟 |
| `15` | 15 分钟 |
| `30` | 30 分钟 |
| `60` | 60 分钟 |
| `101` | 日 K |
| `102` | 周 K |
| `103` | 月 K |

**fqt 复权类型**

| 值 | 含义 |
|----|------|
| `0` | 不复权 |
| `1` | 前复权 |
| `2` | 后复权 |

**返回数据结构**

响应 `data.klines` 为字符串数组，每项逗号分隔，按以下顺序：

```json
["2024-01-02,1000.00,1020.00,1030.00,995.00,50000,510000000,3.5,2.0,20.00,0.8", ...]
```

| 位置 | 字段（fields2） | 含义 | 类型 |
|------|----------------|------|------|
| 0 | `f51` | 日期 | `YYYY-MM-DD` |
| 1 | `f52` | 开盘价 | float |
| 2 | `f53` | 收盘价 | float |
| 3 | `f54` | 最高价 | float |
| 4 | `f55` | 最低价 | float |
| 5 | `f56` | 成交量 | 手 |
| 6 | `f57` | 成交额 | 元 |
| 7 | `f58` | 振幅 | % |
| 8 | `f59` | 涨跌幅 | % |
| 9 | `f60` | 涨跌额 | — |
| 10 | `f61` | 换手率 | % |

---

## 4 参数完整说明

### 4.1 fs 参数

| 类型 | fs 值 | 说明 |
|------|-------|------|
| 行业板块列表 | `m:90+t:2` | 约 497 个行业板块 |
| 概念板块列表 | `m:90+t:3` | 约 468 个概念板块 |
| 地域板块列表 | `m:90+t:1` | 31 个地域板块 |
| 板块成分股 | `b:{BK代码}` | 如 `b:BK0428` |
| 指定个股 | `s:{市场}{代码},...` | 如 `s:1600519,s:0000001` |
| 全市场 A 股 | `m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048` | 沪深主板+创业板 |

### 4.2 fid / po 排序参数

- `fid`：指定排序字段，如 `f3`（涨跌幅）、`f6`（成交额）、`f62`（主力净流入）
- `po`：`1` 降序，`0` 升序

---

## 5 字段完整枚举

### 5.1 行情标识字段

| 字段 | 枚举名 | 含义 |
|------|--------|------|
| `f12` | `CODE` | 代码（股票代码或板块代码） |
| `f14` | `NAME` | 名称 |

### 5.2 行情价格 / 量价字段

| 字段 | 枚举名 | 含义 | 单位 |
|------|--------|------|------|
| `f2` | `PRICE` | 最新价 | — |
| `f3` | `PCT_CHANGE` | 涨跌幅 | % |
| `f4` | `CHANGE` | 涨跌额 | — |
| `f5` | `VOLUME` | 成交量 | 手 |
| `f6` | `AMOUNT` | 成交额 | 元 |
| `f7` | `AMPLITUDE` | 振幅 | % |
| `f8` | `TURNOVER` | 换手率 | % |
| `f9` | `PE` | 市盈率 | — |
| `f10` | `VOL_RATIO` | 量比 | — |
| `f15` | `HIGH` | 最高价 | — |
| `f16` | `LOW` | 最低价 | — |
| `f17` | `OPEN` | 开盘价 | — |
| `f18` | `PREV_CLOSE` | 昨收价 | — |

### 5.3 市值字段

| 字段 | 枚举名 | 含义 | 单位 |
|------|--------|------|------|
| `f20` | `MARKET_CAP` | 总市值 | 元 |
| `f21` | `CIRC_MARKET_CAP` | 流通市值 | 元 |

### 5.4 资金流字段

| 字段 | 枚举名 | 含义 | 板块接口 | 个股接口 |
|------|--------|------|----------|----------|
| `f62` | `MAIN_NET_INFLOW` | 主力净流入 | ✓ | ✓ |
| `f66` | `SUPER_LARGE_INFLOW` | 超大单流入 | ✓ | ✓ |
| `f69` | `SUPER_LARGE_OUTFLOW` | 超大单流出 | ✓ | ✓ |
| `f72` | `LARGE_INFLOW` | 大单流入 | ✓ | ✓ |
| `f75` | `LARGE_OUTFLOW` | 大单流出 | ✓ | ✓ |
| `f78` | `MID_INFLOW` | 中单流入 | ✓ | ✓ |
| `f84` | `MID_OUTFLOW` | 中单流出 | — | ✓ |
| `f81` | `SMALL_INFLOW` | 小单流入 | ✓ | ✓ |
| `f87` | `SMALL_OUTFLOW` | 小单流出 | — | ✓ |

> 超大单：单笔 ≥ 100 万元；大单：单笔 20-100 万元；中单：4-20 万元；小单：< 4 万元

---

## 6 板块代码表

### 6.1 热门行业板块（BK04xx）

| 代码 | 名称 | 代码 | 名称 |
|------|------|------|------|
| BK0420 | 航空 | BK0421 | 铁路公路 |
| BK0422 | 物流 | BK0424 | 水泥 |
| BK0427 | 公用事业 | BK0428 | 新能源车 |
| BK0430 | 整车 | BK0431 | 汽车零部件 |
| BK0432 | 家电 | BK0433 | 消费电子 |
| BK0434 | 电子元件 | BK0435 | 半导体 |
| BK0436 | 电力设备 | BK0437 | 光伏 |
| BK0438 | 风电 | BK0439 | 储能 |
| BK0440 | 电池 | BK0441 | 通信设备 |
| BK0442 | 通信服务 | BK0443 | 计算机硬件 |
| BK0444 | 软件 | BK0445 | IT服务 |
| BK0446 | 互联网 | BK0448 | 传媒 |
| BK0449 | 游戏 | BK0450 | 医疗器械 |
| BK0451 | 医疗服务 | BK0452 | 化学制药 |
| BK0453 | 生物医药 | BK0454 | 中药 |
| BK0456 | 食品加工 | BK0457 | 饮料 |
| BK0458 | 农业 | BK0459 | 化工 |
| BK0463 | 钢铁 | BK0464 | 有色金属 |
| BK0465 | 稀有金属 | BK0466 | 采矿 |
| BK0467 | 煤炭 | BK0468 | 油气 |
| BK0469 | 电力 | BK0471 | 纺织服装 |
| BK0474 | 房地产 | BK0475 | 人工智能 |
| BK0477 | 白酒 | BK0478 | 银行 |
| BK0479 | 券商 | BK0480 | 保险 |
| BK0482 | 国防军工 | BK0483 | 航天 |
| BK0484 | 船舶 | BK0492 | 算力 |
| BK0493 | 云计算 | BK0494 | 大数据 |
| BK0495 | 网络安全 | — | — |

### 6.2 热门概念板块（BK05xx）

| 代码 | 名称 | 代码 | 名称 |
|------|------|------|------|
| BK0537 | 氢能源 | BK0538 | 碳中和 |
| BK0539 | 数字经济 | BK0540 | 元宇宙 |
| BK0541 | ChatGPT | BK0542 | 大模型 |
| BK0543 | 机器人 | BK0544 | 低空经济 |
| BK0545 | 卫星互联网 | BK0546 | 固态电池 |
| BK0548 | 量子计算 | BK0550 | 人形机器人 |
| BK0551 | 自动驾驶 | BK0552 | AIGC |
| BK0559 | 光模块 | BK0560 | CPO |
| BK0561 | 液冷 | BK0562 | PCB |
| BK0563 | HBM | BK0565 | 芯片国产 |
| BK0569 | 核电 | BK0571 | 钠电池 |
| BK0572 | 锂矿 | BK0573 | 稀土磁材 |
| BK0581 | DeepSeek | — | — |

> 完整板块列表见 `data/sector_codes.json`，可通过 `SectorCodeRegistry` 按名称/代码查询。

---

## 7 Python SDK 用法

### 模块结构

```
crawler/eastmoney/
├── __init__.py          # 统一导出所有类/枚举
├── client.py            # EastMoneyClient（HTTP 基础客户端）
├── enums.py             # SectorType, KlineInterval, AdjustType, MarketType,
│                        # QuoteField, FundField, SectorCode,
│                        # SectorCodeRegistry, SectorStocksRegistry, to_secid
├── sector.py            # SectorAPI（板块行情 / 成分股 / 资金流）
├── stock.py             # StockAPI（个股行情 / 资金流）
└── kline.py             # KlineAPI（K线）
```

### 快速上手

```python
from eastmoney import (
    SectorAPI, StockAPI, KlineAPI,
    SectorType, KlineInterval, AdjustType,
    SectorCode, SectorCodeRegistry, SectorStocksRegistry,
    to_secid,
)

# 客户端（可选代理）
# client = EastMoneyClient(proxies={"https": "http://127.0.0.1:7890"})
# 所有 API 类都支持传入 client 参数共享连接
```

### EastMoneyClient

```python
from eastmoney.client import EastMoneyClient

client = EastMoneyClient(
    proxies=None,          # None=跟随系统代理，{}=禁用代理
    timeout=15.0,          # 单次请求超时（秒）
    max_retries=3,         # 失败重试次数
    retry_backoff=0.5,     # 指数退避系数
)
```

### SectorAPI — 板块数据

```python
api = SectorAPI()   # 或 SectorAPI(client=client)

# 板块实时行情列表（全量，自动翻页）
df = api.get_sector_list(SectorType.INDUSTRY)   # 行业板块
df = api.get_sector_list(SectorType.CONCEPT)    # 概念板块
df = api.get_sector_list(SectorType.REGION)     # 地域板块

# 板块成分股列表
df = api.get_sector_stocks("BK0428")            # 新能源车成分股

# 板块资金流向
df = api.get_sector_fund_flow(SectorType.CONCEPT)
```

**`get_sector_list` 返回列**：代码、名称、最新价、涨跌幅(%)、涨跌额、成交量(手)、成交额(元)、振幅(%)、换手率(%)、总市值、流通市值

**`get_sector_stocks` 返回列**：代码、名称、最新价、涨跌幅(%)、涨跌额、成交量(手)、成交额(元)、振幅(%)、换手率(%)、市盈率、量比、最高价、最低价、开盘价、昨收价

**`get_sector_fund_flow` 返回列**：代码、名称、主力净流入、超大单流入、超大单流出、大单流入、大单流出、中单流入、小单流入

### StockAPI — 个股数据

```python
api = StockAPI()

# 实时行情（三种模式）
df = api.get_stock_quote(bk_code="BK0581")                     # 按板块成分
df = api.get_stock_quote(stock_codes=["600519", "000001"])     # 指定股票
df = api.get_stock_quote()                                      # 全市场 A 股

# 个股资金流向
df = api.get_stock_fund_flow(bk_code="BK0550")   # 按板块成分
df = api.get_stock_fund_flow()                    # 全市场
```

**`get_stock_quote` 返回列**：代码、名称、最新价、涨跌幅(%)、涨跌额、成交量(手)、成交额(元)、振幅(%)、换手率(%)、市盈率、量比、最高价、最低价、开盘价、昨收价、总市值、流通市值

**`get_stock_fund_flow` 返回列**：代码、名称、主力净流入、超大单流入、超大单流出、大单流入、大单流出、中单流入、中单流出、小单流入、小单流出

### KlineAPI — K线数据

```python
api = KlineAPI()

# 板块日K（人工智能）
df = api.get_kline("BK0475", interval=KlineInterval.DAY, limit=500)

# 个股日K（贵州茅台，前复权，指定日期范围）
df = api.get_kline(
    "600519",
    interval=KlineInterval.DAY,
    adjust=AdjustType.FORWARD,
    start_date="20230101",
    end_date="20231231",
)
```

**返回列**：日期（datetime）、开盘价、收盘价、最高价、最低价、成交量(手)、成交额(元)、振幅(%)、涨跌幅(%)、涨跌额、换手率(%)

### SectorCodeRegistry — 板块代码查询

```python
from eastmoney import SectorCodeRegistry

# 获取某类板块全量列表
industry_list = SectorCodeRegistry.get_all("industry")
# [{'code': 'BK0420', 'name': '航空'}, ...]

# 按名称关键词搜索（跨类型）
results = SectorCodeRegistry.find_by_name("芯片")
# [{'code': 'BK0565', 'name': '芯片国产替代', 'type': 'concept'}, ...]

# 按代码查找
info = SectorCodeRegistry.find_by_code("BK0492")
# {'code': 'BK0492', 'name': '算力', 'type': 'industry'}
```

### SectorStocksRegistry — 成分股注册表

```python
from eastmoney import SectorStocksRegistry

# 获取板块成分股（需先运行 fetch_sector_stocks.py）
stocks = SectorStocksRegistry.get_stocks("BK0428")
# [{'code': '000625', 'name': '长安汽车'}, ...]

# 查询股票所属全部板块
sectors = SectorStocksRegistry.find_sectors_by_stock("600519")
# [{'bk_code': 'BK0457', 'name': '白酒', 'type': 'industry'}, ...]

# 生成 股票→板块代码列表 映射（用于因子构建）
mapping = SectorStocksRegistry.stock_sector_map("industry")
# {'000001': ['BK0473', 'BK0478'], ...}

# 生成 板块代码→股票代码列表 映射
mapping = SectorStocksRegistry.sector_stock_map("industry")
# {'BK0428': ['000625', '002594', ...], ...}

# 更新数据后强制重新加载
SectorStocksRegistry.reload()
```

### to_secid — 代码转换工具

```python
from eastmoney import to_secid

to_secid("BK0475")  # -> "90.BK0475"  板块
to_secid("600519")  # -> "1.600519"   沪市
to_secid("000001")  # -> "0.000001"   深市
to_secid("300750")  # -> "0.300750"   创业板
to_secid("688981")  # -> "0.688981"   科创板
```

---

## 8 数据文件结构

### data/sector_codes.json

由 `scripts/fetch_sector_enums.py` 生成，记录全量板块代码。

```json
{
  "_meta": {
    "industry": 497,
    "concept": 468,
    "region": 31,
    "source": "https://push2.eastmoney.com/api/qt/clist/get",
    "note": "Run fetch_sector_enums.py to refresh"
  },
  "industry": [
    {"code": "BK0420", "name": "航空"},
    ...
  ],
  "concept": [...],
  "region": [...]
}
```

### data/sector_stocks.json

由 `scripts/fetch_sector_stocks.py` 生成，记录所有板块的成分股。

```json
{
  "industry": {
    "BK0428": {
      "name": "新能源车",
      "stocks": [
        {"code": "000625", "name": "长安汽车"},
        {"code": "002594", "name": "比亚迪"},
        ...
      ]
    },
    ...
  },
  "concept": { ... },
  "region": { ... }
}
```

---

## 9 维护脚本

### scripts/fetch_sector_enums.py — 刷新板块代码

```bash
# 基本用法（跟随系统代理）
python crawler/scripts/fetch_sector_enums.py

# 指定代理
python crawler/scripts/fetch_sector_enums.py --proxy http://127.0.0.1:7890

# 禁用代理
python crawler/scripts/fetch_sector_enums.py --no-proxy
```

输出到 `crawler/data/sector_codes.json`，拉取行业（~497）+ 概念（~468）+ 地域（31）三类板块的完整代码表。

### scripts/fetch_sector_stocks.py — 拉取板块成分股

```bash
# 拉取全部类型（行业+概念+地域）
python crawler/scripts/fetch_sector_stocks.py --proxy http://127.0.0.1:7890

# 仅拉取行业板块
python crawler/scripts/fetch_sector_stocks.py --proxy http://127.0.0.1:7890 --type industry

# 断点续传（跳过已有数据）
python crawler/scripts/fetch_sector_stocks.py --proxy http://127.0.0.1:7890 --resume
```

- 每个 HTTP 请求使用独立 curl 进程（新 TCP 连接），规避代理连接复用问题
- 每类板块拉完自动保存一次（防崩溃丢数据）
- 输出到 `crawler/data/sector_stocks.json`

**推荐工作流**

```bash
# 1. 先刷新板块代码
python crawler/scripts/fetch_sector_enums.py --proxy http://127.0.0.1:7890

# 2. 再拉取成分股（行业板块优先，耗时较长）
python crawler/scripts/fetch_sector_stocks.py --proxy http://127.0.0.1:7890 --type industry
python crawler/scripts/fetch_sector_stocks.py --proxy http://127.0.0.1:7890 --type concept --resume
```

---

*最后更新：2026-03-08*
