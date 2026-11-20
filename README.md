# 升级包下载网关（合并段拉取 + 分块校验 + 双镜像切源）

FastAPI 实现的升级包下载网关。网关把相邻的 64 KiB 块**合并成段**，一次 HTTP Range
请求向主镜像拉取最多 4 块：97 块的整包在主镜像正常时只需 **25 次镜像请求**。段数据
边收边按清单**逐块核对范围、长度与 SHA-256**，校验通过的块立即按序发给客户端，不等
整段收齐。合并响应断流或读到坏块时，保留已校验的前缀，从首个未通过的块起按原来的
主备规则**逐块补齐本段**；两台镜像都取不到可信块时中断响应，客户端因收到的字节数少于
声明长度而识别出下载失败。

## 组成

| 服务 | 说明 | 端口 |
| --- | --- | --- |
| `gateway` | 下载网关 + 网页 | 宿主 `8000` |
| `primary-mirror` | 主镜像，支持 Range 与故障注入 | 仅内网 |
| `backup-mirror` | 备用镜像，同上 | 仅内网 |

- 升级包为确定性生成的 6,303,801 字节（96 个 64 KiB 块 + 1 个 12,345 字节末块），
  清单 `app/manifest.json` 内置每块的 `offset / length / sha256` 及整包摘要。
- 两个镜像容器首次启动时按同一清单生成文件并核对整包 SHA-256；数据落在仓库内
  `data/primary`、`data/backup`（绑定挂载，已预留可写权限）。
- 网关每个下载**最多在内存里暂存 2 块**：正在接收的一块 + 正在发送的一块也算在内
  （信号量在收块前占位、交给 ASGI 后释放）。
- 合并因子由 `SEGMENT_CHUNKS` 控制（默认 4，`ceil(97 / 4) = 25` 次请求）。

## 启动

```bash
docker compose up --build -d
# 打开 http://127.0.0.1:8000/
```

网页可以：

- 直接下载升级包（浏览器保存，`/download/<包名>`）；
- 「下载并校验」：在页面内逐块接收、核对收到的总长度与整包 SHA-256，并显示进度与
  成功/失败结论；
- 查看两台镜像状态，对任一镜像设置从第 N 块起 `truncate`（只发半块后断流）或
  `corrupt`（返回等长坏块），或恢复正常。

命令行验收（会临时改故障设置，结束时自动恢复）：

```bash
python3 scripts/smoke.py --base http://127.0.0.1:8000
```

也可以在启动时直接让镜像带故障（便于复现切源）：

```bash
PRIMARY_FAULT_MODE=truncate PRIMARY_FAULT_FROM_CHUNK=2 docker compose up --build -d
# 两个变量：PRIMARY_FAULT_MODE / PRIMARY_FAULT_FROM_CHUNK、BACKUP_FAULT_MODE / BACKUP_FAULT_FROM_CHUNK
```

## 接口

网关：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/` | 网页 |
| GET | `/api/manifest` | 包名、大小、分块、整包 SHA-256 |
| GET | `/download/{name}` | 分块校验下载；整包长度在 `Content-Length` 中预先声明 |
| GET | `/api/mirrors` | 两台镜像的在线状态与故障设置 |
| POST | `/api/mirrors/{primary\|backup}/fault` | `{"mode":"truncate\|corrupt\|none","from_chunk":N}` |
| DELETE | `/api/mirrors/{primary\|backup}/fault` | 恢复正常 |
| GET | `/api/stats` | 下载计数、主/备取块数与 **Range 请求数**、拒绝原因、最近一次下载详情 |

镜像：

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/files/{name}` | 支持单段 `Range`，返回 206 / 416 |
| GET | `/admin/state` | 角色、故障设置、包信息 |
| POST / DELETE | `/admin/fault` | 故障注入 / 恢复 |

## 下载失败时客户端如何识别

响应一开始就声明整包 `Content-Length`。若第 N 块两台镜像都取不到正确内容，网关在
发出已校验的前 N 块后中止连接：

- 若首块就失败，网关直接返回 `502` JSON（含 `detail`、`download_id`、具体原因）；
- 若在流中途失败，浏览器/客户端会看到 `IncompleteRead` 或
  `terminated` 一类错误，已收字节数严格小于声明长度；网页会显示「失败」、
  实际中断位置与声明长度。

网关不会把未经校验的字节转发给客户端。

## 实测结果

`docker compose` 无法在本机运行（环境无 Docker 守护进程），因此用**与镜像完全相同的
uvicorn 启动命令**在本机起三份服务实测；Compose 文件已用官方 compose-spec JSON
Schema 校验通过。

| 场景 | 结果 |
| --- | --- |
| 正常下载 | 200，6,303,801 字节，SHA-256 `4252eb5e…9741a4` 一致；**主镜像 25 次、备用 0 次 Range 请求**，最大暂存 2 块 |
| 主镜像从第 6 块起 `truncate`（段 4..7 中途断流） | 段 0 整块 + 各段已校验前缀保留（主镜像共 50 块），剩余 47 块逐块改走备用镜像；主/备请求 72/47 次，整包摘要一致 |
| 主镜像从第 2 块起 `corrupt` | 坏块全部被逐块摘要拦下并逐块切备用镜像，客户端收到的仍是正确文件 |
| 主镜像完全离线 | 全部 97 块由备用镜像提供（备用 97 次请求，主镜像 0 次到达），整包一致 |
| 下载途中主镜像进程被关停 | 在途合并段按传输错误处理，后续块改走备用镜像，整包一致 |
| 两台镜像从第 10 块起分别 `corrupt` / `truncate` | 客户端在 655,360 字节（第 10 块处）收到断流，识别为失败 |
| 首块两台都失败 | 网关返回 502 JSON，网页/脚本识别为失败 |
| 6 个并发下载 + 主镜像全坏 | 全部成功，单下载最大暂存块数始终为 2 |
| 客户端下载途中主动断开 | 未读完的镜像响应被关闭（`mirror_streams_open` 归零），统计记为 failed 并记录「client disconnected」 |

自动化测试（35 项）覆盖清单/Range 解析、合并段分组、逐块校验、段中途断流与段内坏块
的前缀保留、两种切源、双源失败、镜像半途关停、客户端取消关闭镜像响应、并发、网页接口
与故障语义：

```bash
pip install -r requirements.txt pytest
python -m pytest -q
```

网页流程另外用真实网关 + jsdom 跑通：正常下载、主镜像故障时切源成功、双源故障时
页面显示「失败」并给出中断字节数。

## 设计说明

- **合并段读取**：相邻块按 `SEGMENT_CHUNKS`（默认 4）分组为段，一次严格单段 Range
  （`bytes=段首-段末`）拉取。要求 206、`Content-Range` 与请求完全一致、`Content-Length`
  等于段长；段体内每收满一块即核对该块在清单中的 SHA-256，通过立即发客户端。
- **段失败逐块补齐**：合并响应断流、传输错误或某块摘要不符时，已校验并入队的前缀
  原样保留，从首个未通过的块起对该段余下部分逐块执行原来的主备规则（先主后备、
  整块重取、双源皆败则中断下载）。后续段仍先尝试合并段请求。
- **有界暂存**：信号量在收块前占位、在数据交给 ASGI 后释放，正在接收和正在发送的块
  都计入，单下载驻留不超过 2 块；客户端变慢时背压一路传导到镜像读取。
- **取消即关流**：客户端中途断开时，生产协程被取消，正在读取的镜像响应随
  `httpx` 流上下文关闭（连接不复用），`/api/stats` 的 `mirror_streams_open`
  归零可观测。
- **失败可观测**：`/api/stats` 记录每次下载向主/备镜像发出的 **Range 请求数**
  （`last_download.requests`）、每块来源、段级与块级回退明细、已发字节与最大暂存块数；
  网页「网关统计」顶部直接显示最近一次下载的主/备请求次数。
- 清单由 `app/artifact.py` 生成并可复现：`python -m app.artifact --output /tmp/pkg.bin`。

## 目录

```
app/
  artifact.py     确定性升级包与清单生成/校验
  manifest.json   内置 64 KiB 分块清单
  ranges.py       严格单段 Range 解析
  gateway.py      下载网关（合并段拉取、逐块校验、切源、有界暂存、API、统计）
  mirror.py       镜像（Range、故障注入）
  static/         网页
tests/            35 项端到端/单元测试
scripts/smoke.py  对运行中网关的验收脚本（含合并请求数检查）
Dockerfile        网关镜像
Dockerfile.mirror 镜像服务镜像
docker-compose.yml
data/             镜像文件落盘目录（运行时生成）
```

## 未完成 / 已知限制

- **未在真实 Docker 中验证**：本机无 Docker 守护进程，`docker compose up` 与镜像构建
  未实际执行；已校验 Compose schema、按镜像命令本地起服务、用 `.dockerignore` 核对
  构建上下文。首次在有 Docker 的机器上运行仍应复核 `docker compose up --build`。
- 未做熔断：某段合并请求失败后，后续段仍会先尝试主镜像的合并请求再逐块回退，主镜像
  持续故障时每次下载会多出一批必然失败的主镜像请求（计入 `primary_errors`，不影响
  正确性）。
- Range 请求数在镜像开始应答时计数；连接根本未建立（如对端离线）的尝试只计入
  `primary_errors` / `backup_errors`，不计入 `requests`。
- 故障注入状态只存在进程内存里，容器重启即恢复；镜像内容随 `data/` 持久化。
- 网关同一时刻只对接一个主镜像 + 一个备用镜像，未做镜像池、健康探测或按延迟择优。
- 下载连接的 `Content-Length` 固定为整包长度，因此网关不支持客户端对**整包**发 Range
  断点续传（`Accept-Ranges: none`）；续传能力体现在镜像侧按块取数。
- 未做鉴权/限流/TLS，适合本地或内网演示；`/api/mirrors/*/fault` 也无保护。
- 统计为进程内计数，未持久化，也未按下载 ID 关联日志。
