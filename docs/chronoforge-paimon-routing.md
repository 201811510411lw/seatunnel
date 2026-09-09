# Paimon 固定分桶写入正确性（Issue #113）

## 实现边界

本修复面向 `dev-liwei` 的 SeaTunnel 2.3.13、Paimon 1.1.1，以及
Flink 1.15 Adapter 在 Flink 1.18.1 上的运行链路，不升级依赖的生产版本。
它不是 Paimon 内核修复，也不是通过调整 CDC 阶段或强制 Sink 单并发规避问题。

`SeaTunnelSink.getWriteRouting()` 是可选的 Connector 路由契约，默认不改变其他 Sink。
Paimon 使用已经加载的物理表 schema，经 `RowConverter`、
`FixedBucketRowKeyExtractor` 和 `ChannelComputer.select` 计算 Writer。
Flink `SinkExecuteProcessor` 在 Sink 之前插入 custom partition 边，保持明确的 Sink
并行度。Writer 再校验收到的行确实属于自己的 bucket，错误归属直接失败。

此版本 Flink `SinkV1Adapter` 在仅有 Global Committer、没有本地 Committer 时选择
不保存 Writer 状态的适配分支。对需要路由且同时具有 Writer 状态和全局提交的 Sink，
`FlinkSink` 提供无副作用的本地 Committer，让适配器选择有状态的两阶段分支；真实
落盘提交仍由原有 Global Committer 执行。恢复上下文有 Checkpoint、却没有 Writer
状态时直接拒绝，防止旧状态或空扩容分配绕过版本检查。

多表路由先按输入 `tableId` 找到实际 Paimon 表路由，再按分区和 bucket 分发。
独立物理表使用独立 Writer；同一个多表 Sink 内两个源表映射到同一物理目标时拒绝启动，
应在上游合并后只建立一套目标 Writer。

源端仍必须保证同一主键的 CDC 事件顺序，以及 Snapshot 完成 Checkpoint 后才切换增量。
本实现保证 Writer 归属，不会为任意乱序的多个生产者重建源端事件顺序。
同一个物理目标不得被另一个独立 Sink 块、作业或外部 Writer 并发写入；跨作业互斥不在本路由契约内。

## 支持与拒绝

| 场景 | 行为 |
| --- | --- |
| 固定 bucket=1 或多 bucket | 支持；同表同分区同 bucket 只归属于一个 Writer |
| 多表、不同分区、复合主键、自定义 bucket-key | 使用 Paimon 实际 schema 分桶，不替换成业务主键 hash |
| Source/Sink 并行度大于 1 | 保留；非分区 bucket=1 的单表本身只有一个可安全写入的 bucket |
| `multi_table_sink_replica=1` | 支持，保留原有多表队列与 Checkpoint 提交 |
| `multi_table_sink_replica>1` | 拒绝，避免内部 replica 再次破坏 bucket 归属 |
| 动态 bucket、跨分区 upsert、bucket-unaware | 当前 Flink 路由明确拒绝，不静默退回旧路径 |
| Schema/flush 控制事件 | 在路由端明确拒绝；在线 Schema Evolution 尚未协调广播和路由 schema 更新 |
| 同版本、相同 Sink 并行度的状态恢复 | 保留 Writer 状态序列化及 Checkpoint 提交机制 |
| routing state v2 从两个 Sink Writer 恢复至一个 | 收集全部旧 Writer 状态，校验身份和提交边界后合并恢复 |
| routing state v1 相同并行度恢复 | 保留；生成新暂停点后状态升级为 v2 |
| 无路由版本的旧状态、v1 改并行度、扩容或缩容后仍大于 1 | 明确拒绝，不承诺任意扩缩容 |

Writer 状态包含 `bucketRoutingVersion`、`writerParallelism` 和 `writerIndex`，保留 Java 序列化 UID。
新状态使用 routing version 2；缩容至单 Writer 时必须恰好收齐原并行度数量的状态，
旧 Writer 下标完整且无重复，版本、并行度、commitUser、Checkpoint 边界一致。
空闲 Writer 也必须提供状态，不能用没有 CommitMessage 判断其状态可以丢弃。
多表恢复按表标识收集所有旧 Writer，而不是只查找新 subtask 的下标；缺表或变更表拓扑拒绝。
Flink 包装状态的 Checkpoint 边界也必须一致；其他非路由 Sink 保留原恢复行为。
旧状态缺少路由版本标记，必须拒绝；不能仅凭旧状态能够反序列化就认为它安全。
改变 Source 并行度仍须遵守 ChronoForge 的 Snapshot/UNKNOWN 保护，本修复不修改平台守卫。
运行期间禁止外部修改目标 schema、bucket 配置或写入布局。

## 本地验证

使用 JDK 8：

```bash
export JAVA_HOME=/home/lsym005226/project/jdk1.8.0_202
export PATH="$JAVA_HOME/bin:$PATH"

mvn -f seatunnel-api/pom.xml -Dskip.spotless=true test install
mvn -f seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-common/pom.xml \
  -Dskip.spotless=true -Dtest=FlinkSinkWriterCheckpointBoundaryTest install
mvn -f seatunnel-core/seatunnel-flink-starter/seatunnel-flink-starter-common/pom.xml \
  -Dskip.spotless=true test install
mvn -f seatunnel-connectors-v2/connector-paimon/pom.xml \
  -Dskip.spotless=true test package
```

Connector 测试依赖本地安装的配套 Starter/API。Flink 1.18.1 仅在 Connector 的
`test` 依赖中用于 MiniCluster，不打入生产 Connector。

`PaimonCheckpointCommitTest` 覆盖 DELETE、连续 UPDATE、状态序列化恢复、旧状态和
缩容合并、待提交数据恢复、重复恢复与 Global Committer 重放幂等、
缺失/重复/不一致状态拒绝、错误 Writer 拒绝、Schema 事件拒绝。
同并行度多 Writer 恢复不独立提交各自片段：首次写入、prepareCommit 或 snapshotState
等待 Global Committer 完整提交旧边界，再重新加载 TableWrite。等待上限取 30 秒与
Paimon commit.timeout 的较小值；超时、中断或无法确认均拒绝继续。
无实际 data/compaction/index 增量的空提交不等待，即使消息列表非空；Paimon 默认忽略
空提交，不保证生成对应 Checkpoint 的 snapshot。未知消息类型仍保守等待。
回归通过与已提交内容不同的 pending 值逐键检查，避免仅靠行数掩盖片段丢失。
Flink Global Committer 对 routed Sink 在恢复过滤阶段重放完整提交消息，成功后才过滤；
不能直接丢弃恢复消息让 Writer 永远等待。恢复提交失败或要求重试时拒绝恢复。
`MultiTableWriteRoutingTest` 覆盖多表选择、重复物理目标、replica 与混合路由拒绝。
`PaimonFlinkBucketRoutingTest` 经真实 `SinkExecuteProcessor`、Flink Adapter、
Paimon Connector 和本地 Paimon 读回，覆盖双 Source/双 Sink、Snapshot Checkpoint
到增量、单桶、多表多分区多桶及 Savepoint 恢复。
新增缩容用例从真实双 Writer Savepoint 恢复至单 Writer，暂停期间在独立 Paimon Writer
中执行 compaction，再持久化 `write-only=false`，重建 Sink 以读取新表属性。
恢复后的合成 Source 等待三次成功 Checkpoint 通知，再发送 INSERT/UPDATE/DELETE 并读回断言。
这些是本地文件系统上的 Flink MiniCluster 验证，不等同于 Kubernetes/MySQL CDC/S3 现场验收。

## 全量后降并行度的操作边界

先发布配套 API、Flink Adapter、Starter 与 Connector 的同一版本运行镜像。
本地代码、JAR 或测试通过均不表示线上镜像已经更新。

1. 在测试表以并行度 2 全量运行，确认 Checkpoint 已确认增量阶段。
2. 使用平台 Savepoint 暂停，保留实际暂停点路径，等待 Writer 完全停止。
3. 对同一物理目标独立 compaction，全部成功后逐表设置 `write-only=false`。
4. 保持 Source、目标表、bucket 配置和算子身份不变，从同一暂停点以并行度 1 恢复。
5. 至少连续三次 Checkpoint 成功，验证恢复后的增删改和约定的数据一致性。
6. 首次只降并行度，内存与 CPU 缩减另行验证。

不支持旧无路由状态直接缩容，也不使用 `allowNonRestoredState` 丢弃状态绕过保护。
暂停期间不运行其他业务 Writer；独立 compaction 结束后才能恢复。

旧路径对照仅用于测试，不存在生产配置开关：

```bash
mvn -f seatunnel-connectors-v2/connector-paimon/pom.xml \
  -Dskip.spotless=true -Dissue113.test.legacy-routing=true \
  -Dtest=PaimonFlinkBucketRoutingTest#shouldRouteSingleBucketThroughActualFlinkSink test
```

该命令在测试 Sink 中省略路由，重现旧 Adapter 的直接连接；产品断言仍要求 DELETE
无残留、UPDATE 为最新值，因此预期失败。正常验证不设置该属性。

2026-09-09 的本地对照已在真实 Flink 链路得到 `expected: <99> but was: <100>`，
即旧路径删除后仍残留一行。受管直接 Connector 测试同样先观察到 DELETE 断言失败。
这些测试使用合成数据和本地文件系统，不连接生产 MySQL、Paimon 仓库或 Kubernetes。
Savepoint 测试通过合成可恢复 Source 模拟 Snapshot Checkpoint 后切换增量，
不等同于已经验证完整 MySQL CDC Source 或生产 Operator 发布。

Issue #113 路由基线的本地验证结果（2026-09-09，后续 #114 缩容测试另计）：

| 模块 | 测试数 | 构建 |
| --- | ---: | --- |
| `seatunnel-api` | 76 | `install` 通过 |
| `seatunnel-translation-flink-common` | 3 | `install` 通过 |
| `seatunnel-flink-starter-common` | 1 | `install` 通过 |
| `connector-paimon` | 57 | `package` 通过 |
| `seatunnel-flink-15-starter` | 3 | `package` 通过 |

合计 140 项通过。最终 API/Starter 收窄保护范围后，额外重跑 3 项 Paimon Flink
链路测试全部通过。修改的 Java 与 Connector POM 的 Spotless 检查、`git diff --check`
通过；未修改的其他模块 POM 存在既有格式差异，没有为了全仓格式检查而改动它们。
未执行根 reactor 全量测试、完整 MySQL CDC 端到端验收或 Jenkins。

## 发布与历史数据

Issue #114 缩容修复最终本地验证（2026-09-09）：API 78、Flink Adapter 7、Starter Common 1、
Paimon 73、Starter 15 3，共 162 项通过。其中 Paimon Checkpoint 20 项、真实 MiniCluster
5 项，包含相同并行度恢复、空提交边界、单桶及多表多分区多桶的 2 -> 1 恢复。
Standards/Spec 双轴 Review 的阻塞项已修复；源码测试通过不代表运行镜像或现场验收完成。

本地修改不等于镜像已集成、部署或生产验收。新增 API、Starter 和 Paimon Connector
必须配套构建与发布，不能只替换 Connector；不能继续沿用旧 Connector 摘要宣称已修复。
ChronoForge 的锁定补丁/镜像装配链仍需单独集成，禁止覆盖其已有 Transform 工作区修改。

本补丁不清除历史 DELETE 残留，也不纠正已经落盘的旧值覆盖。旧作业升级应先明确
停写边界、源恢复点、目标表范围与重建方案，再经用户授权执行干净目标表全量重建。
不要从旧 Savepoint 强制恢复、忽略未匹配状态或仅删除平台任务后复用脏目标表。
提交、推送、镜像发布、生产重建及关闭 Issue 都是独立授权步骤。
