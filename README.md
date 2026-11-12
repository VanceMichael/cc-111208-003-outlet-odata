# outletwalk

品牌门店与服务时段 OData 页面查看器。

## 本地运行

使用 Python 3.11 标准库，无 pip 依赖。所有来源与业务数据都是本地合成数据，不连接真实用户系统。

```sh
python -m scripts.materialize
python -m outletwalk peek fixtures/outlets.json
python -m compileall -q .
python -m unittest discover -s tests -v
python -m scripts.demo
```

检查命令输出单份来源的结构摘要。`capture URL --method GET --state var` 仅接受 loopback HTTP，保存原始响应为 SHA256 文件，并在 SQLite captures 表登记；可用 `--header 'Name:Value'` 传头、`--body FILE` 传正文，不跟随重定向。检查器不执行多页、业务去重或正式接收。

## 门店 walk 业务流程

`walk` 在 `peek` 的单份摘要之外实现正式的多页采集与完整性账本，两者边界互不重叠。

```sh
python -m outletwalk walk http://127.0.0.1:8765/outlets \
  --state var/walk --origin http://127.0.0.1:8765 \
  --max-pages 1000 --concurrency 2
python -m outletwalk resume --state var/walk --max-pages 1000
python -m outletwalk status --state var/walk
python -m outletwalk query --state var/walk A
```

- 根链 `@odata.nextLink` 与每家门店的 `Slots@odata.nextLink` 是互不替代的续页链；外层根页取完不代表整次采集完整。
- 链接是不透明值，按产生它的响应地址用 `urljoin` 解析（保留查询串，不手拼 skip），解析后仍限制在配置 `--origin`，跨 origin、循环链接、缺业务键、空续页都有界失败并写入原因。
- 单店完整性与整次采集完整性分开记账：已取齐的门店立即可供 `query`，未取齐的门店在链上记录卡住的下一页 URL 与原因；一家店的子链失败不会撤销其他已确认门店。
- 根记录与子集合各有版本。门店根版本变化会建立独立子代际，迟到的旧版子页进入 `isolated_pages` 隔离，绝不拼入新代际；子页 `owner`/`version` 必须与发起任务时一致。
- 时段归属只按父门店身份加子版本确认，不依赖返回顺序；跨父门店允许相同 `slotId`，同一门店在多个根页重复、子页重复时段都按业务键去重。
- 所有页面、链状态、版本、访问集合、代际与隔离页都落到 `--state` 下的 SQLite（`walk.sqlite3`）。并发受 `--concurrency` 限制，进程结束后用 `resume` 基于账本续行，而不是依赖进程内待办数。

### 状态语义

| 状态 | 含义 |
| --- | --- |
| `complete` | 根链与所有当前代际子链都有界结束 |
| `paused` | 仍有 open 链（页面预算用尽或进程被中断），可 `resume` |
| `failed` | 根链或某当前代际子链有界失败，原因记录在链上，其他门店不受连坐 |
| `superseded` | 门店进入新根版本后，旧代际链与门店行保留可审计但不再是当前数据 |


## 可复现容器

```sh
docker build -t outletwalk:local .
docker run --rm --network none outletwalk:local python -m unittest discover -s tests -v
docker run --rm --network none outletwalk:local
```

镜像固定基础摘要。构建在正式运行前完成；运行和测试不安装包、不下载数据。`python -m scripts.demo` 先让外层根链结束并留一条门店子链未结束，再重启续行补齐，查询结果可直接看出这两步的完整性差别，通常在 10 秒内、控制在 90 秒以内；它演示一个真实业务闭环。

## 协议与来源

- docs/source-contract.md 给出支持子集、字段及失败语义。
- docs/acceptance.md 给出业务验收边界。
- fixtures/source.json 是本地可重放 HTTP 路由；`python -m scripts.source_server --port 8765` 启动，`/__requests` 可检查实际请求。
- scripts/materialize.py 将版本化 base64 二进制夹具还原为原始字节，重复执行安全；二进制文件不是用户附件。
- journal.py 只记录原始响应，不代替新增流程自己的事务和恢复设计。
