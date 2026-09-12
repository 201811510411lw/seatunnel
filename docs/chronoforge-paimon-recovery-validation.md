# Paimon 历史提交恢复改造与验证

关联 [ChronoForge Issue #118](https://github.com/201811510411lw/ChronoForge/issues/118)。207 项 clean 测试、真实 MySQL 自动故障恢复及显式 Savepoint 恢复均已通过。局部基准分配量不变，耗时差异存在噪声；8 组真实 MySQL 旧/新适配器 A/B 全部通过，p=4 全量可见耗时中位数下降 3.40%，样本范围重叠，不能认定稳定提速。

## 交付范围与协议契约

改造基线为 `1d7316aecbd597209dc28299a3604ee621f1cdd4`。目标运行组合为 SeaTunnel 2.3.13、Flink 1.18.1、Paimon 1.1.1；生产适配代码继续按既有 Flink 1.15.3 接口编译。本轮不升级生产依赖。

恢复循环等待发生在中间 Committer 晚于 Global Committer 初始化重放历史消息时：旧适配路径等待下一个 Checkpoint，Writer 又等待旧 Paimon 提交完成。现在由 SeaTunnel Flink 适配层集中处理恢复消息，保留 Flink 原 Writer、本地 Committer、算子身份和路由，替换末端全局提交协调。

提交契约如下：

1. 全局提交必须同时具备 Flink Checkpoint 完成证明、全部生产者的声明和完整消息。空闲 Writer 也属于完整性检查的一部分。
2. 同一边界先合并所有份额，再发布 Paimon 提交；多个历史边界按顺序推进，缺失的旧边界不能被新边界越过。
3. 恢复时固定的 Checkpoint 编号及以前的完整批次调用既有 `restoreCommit`；晚到的恢复消息可以直接推进补交，无需新 Checkpoint 先完成。后续正常批次调用普通 `commit`。
4. Paimon 仅在恢复提交中关闭忽略空提交，发布最新空边界的完成快照。Writer 无条件等待自身保存的边界确认，再重新打开 TableWrite；最新数据文件列表为空不能证明更早全局提交已完成。
5. 提交失败、要求重试、消息缺失、身份不一致或无法确认提交时明确失败，不丢弃状态。Flink 外层 Checkpoint 与 Paimon 内层提交标识独立编号。

恢复额外成本发生在提交边界：尚未发布的空恢复边界可能增加快照元数据及 Paimon 原有过期处理。重复恢复已发布边界应被幂等过滤；常态空提交不因此持续创建快照。逐行路由和转换路径没有新增远端检查。

## 状态兼容与一次性迁移声明

完整契约及配置示例见 [恢复协议与迁移限制](chronoforge-paimon-recovery.md)。首次读取非空旧全局状态 v2，须从恢复点 metadata 核实原 Writer 并行度和 Checkpoint ID，在 `env.paimon.legacy-state` 声明 `writer-parallelism`、`checkpoint-id`；声明必须匹配原布局和实际恢复点，首次升级保持原并行度。完全空 v2 和新全局状态 v3 不需要该声明。

部分 v3 状态先按原并行度完成恢复，再暂停并执行支持的缩容。原 Writer routing v1/v2、Source 阶段和多表完整性守卫保留；不支持任意扩缩容。持续 CDC 使用终止前 Checkpoint 或不 drain 的 Savepoint；恢复状态包含 `Long.MAX_VALUE` 时在任何补交前拒绝，不支持把 drain/terminal 状态恢复为持续作业。

## 干净构建与复现命令

在候选 SeaTunnel 工作树根目录执行，构建环境配置 JDK 8 和匹配的 Maven 依赖仓库。以下五条构建命令对应已读回的最终 clean 日志；不额外计入未执行的模块测试。

```bash
mvn -f seatunnel-api/pom.xml -Dskip.spotless=true clean test install
mvn -f seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-common/pom.xml -Dskip.spotless=true clean test install
mvn -f seatunnel-core/seatunnel-flink-starter/seatunnel-flink-starter-common/pom.xml -Dskip.spotless=true clean test install
mvn -f seatunnel-connectors-v2/connector-paimon/pom.xml -Dskip.spotless=true clean test package
mvn -f seatunnel-core/seatunnel-flink-starter/seatunnel-flink-15-starter/pom.xml -Dskip.spotless=true clean test package
git diff --check
```

`skip.spotless=true` 仅用于上述构建：格式已由主流程对本轮文件单独处理，不能把构建日志中的 `Spotless check skipped` 描述为格式检查通过。最终对本轮 19 个 Java 文件执行受影响四个模块的 `spotless:check` 均通过，`git diff --check` 通过；日志为 `/tmp/cf-recovery-last-verification/spotless-check.log`。

本轮核实的五份最终 clean 日志合计 **207 项测试，失败 0、错误 0、跳过 0，全部 BUILD SUCCESS**：

| 模块 | 测试数 | 日志 |
| --- | ---: | --- |
| API | 79 | `/tmp/cf-recovery-final-api.log` |
| Flink translation common | 29 | `/tmp/cf-recovery-final-adapter.log` |
| Flink starter common | 1 | `/tmp/cf-recovery-final-starter-common.log` |
| Paimon Connector | 95 | `/tmp/cf-recovery-final-paimon.log` |
| Flink 15 starter | 3 | `/tmp/cf-recovery-final-starter15.log` |

上表只计入这五份已读回日志，不把额外命令或尚未执行的集成测试计入通过数量。

## 正确性测试矩阵

| 契约或场景 | 验证入口 | 当前结果 |
| --- | --- | --- |
| 四 Writer 非空本地待提交，恢复无需新 Checkpoint 先行 | `PaimonFlinkPendingRecoveryTest` | 通过；完整主键和值、后续 I/U/D、连续 Checkpoint |
| 原适配器创建真实非空 Checkpoint，新适配器恢复 | `PaimonFlinkLegacyRecoveryTest` | 通过；显式恢复点与迁移声明匹配 |
| 算子身份、UID、并行度和拓扑形状 | `FlinkRecoverySinkTopologyTest` | 通过；p=1/4，显式与自动 UID |
| 声明完整性、乱序、缺份额、旧状态拒绝、序列化往返 | `FlinkGlobalCommitStateSerializerTest`、`FlinkRecoveryGlobalCommitterOperatorTest` | 通过 |
| 重试保留、恢复和正常提交分流、terminal 状态拒绝 | `FlinkRecoveryGlobalCommitterOperatorTest` | 通过；终止状态恢复前不额外发布 |
| 最新为空且更早非空、空边界幂等、恢复前不发布、完整键值 | `PaimonCheckpointCommitTest` | 通过；包含单桶与多桶 |
| 多表、分区、bucket、同并行度暂停恢复及 2→1 | `PaimonFlinkBucketRoutingTest` | 5 项通过，包含独立 compaction 后恢复 |
| MultiTable 恢复调用转发与普通提交隔离 | `MultiTableSinkAggregatedCommitterTest` | 通过 |
| 隔离真实 MySQL 自动故障恢复、两表各 20,000 条完整主键字段和三轮 I/U/D | `PaimonMySqlCdcRecoveryIT` 最终执行 | 通过；自动恢复 Checkpoint 3 |
| 真实 MySQL 显式 Savepoint 恢复及暂停期间变化追赶 | 同一 IT | 通过；Savepoint 42 → 实际恢复 42，恢复后完成 14 次 Checkpoint |

真实 MySQL 最终记录 `result=PASS`。Savepoint metadata 中观察器有 3 个 finished 分片，预期活动状态分片 1 个，实际恢复 1 个；修正后的观察器断言已重跑通过。暂停期间变化追赶读取 8 条记录，耗时 5,604 ms；恢复前后每个边界均核对完整主键和字段，三轮 I/U/D 通过。该记录证明隔离环境的恢复正确性，单次耗时不用于新旧性能比较。

该最终可靠性测试创建的两张隔离源表已清理，清理记录为 `tablesRemoved=2`、`verifiedAbsent=true`。

最终对 `PaimonFlinkPendingRecoveryTest` 仅增加恢复提交委托计时后，单测及 clean package 通过，日志为 `/tmp/cf-recovery-last-verification/pending-timing-and-package.log`。实际 Paimon `aggregated.restoreCommit` 成功调用 1 次，耗时 **1,569,042,532 ns（约 1.569 s）**。这是 JDK 8 冷启动、本地合成 128 行场景下的单次补交耗时；没有旧路径可成功完成的耗时基准，不作加速比较。整个测试耗时 13.775 s，包含其他步骤，不能与补交计时或完整作业重启耗时混用。该额外执行单独留证，不改变上述五份 clean 日志合计 207 项的统计口径。

## shaded 类和最终制品验证

曾出现两个多表恢复超时，原因是测试 classpath 靠前的旧 shaded Starter 内含旧 `MultiTableSinkAggregatedCommitter`，遮蔽靠后的新版 API。旧类没有 `restoreCommit` override，恢复调用回退为普通提交，因此没有发布空恢复快照。该问题通过干净重建 API、适配层及承载其类的 Starter 解决，没有为此修改恢复协议。

`/tmp/cf-recovery-final-artifact-check.jsonl` 已读回通过：测试实际 classpath 与最终 Starter 中发现的三处关键恢复类均与本轮模块 `target/classes` 完整字节一致。检查范围是 `MultiTableSinkAggregatedCommitter`、`FlinkGlobalCommitter`、`PaimonAggregatedCommitter`；后者来自 Connector，不要求被嵌入 Starter。

可复查命令（`RECOVERY_PYTHON` 指向本机约定的审计 Python 环境）：

```bash
"$RECOVERY_PYTHON" \
  /tmp/verify-paimon-recovery-classpath.py \
  --worktree . \
  --surefire seatunnel-connectors-v2/connector-paimon/target/surefire-reports/TEST-org.apache.seatunnel.core.starter.flink.execution.PaimonFlinkBucketRoutingTest.xml \
  --jar seatunnel-core/seatunnel-flink-starter/seatunnel-flink-15-starter/target/seatunnel-flink-15-starter.jar
```

脚本按 classpath 顺序列出分身及 CRC32，并以完整字节相等判定，不能只验证独立 API JAR 或源码。最终重新打包后的测试实际 classpath、Starter 与 Connector 再次逐字节核验通过，记录为 `/tmp/cf-recovery-last-verification/artifact-readback.jsonl`；源码身份、完整 JAR 摘要和镜像读回由独立交付记录绑定。

构建机需同时更新本轮对应源码工作副本和运行时注入 JAR：仅覆盖 JAR 会在后续旧源码重建时退回旧实现；仅覆盖源码则不能证明本次镜像已经包含新类。该维护分支已经直接包含 runtime-r10 既有补丁，**不得直接重新应用 runtime-r10 旧补丁覆盖新实现**。应从本轮固定源码干净构建，核对 API、Starter、Connector 及各 shaded 分身，再进入既有镜像流程。

镜像发布、实际 Pod `imageID`、原运行实例恢复与业务验收分别留证。本地源码和 JAR 核查不证明镜像已部署。本轮文档不包含业务运行标识、数据库标识、GTID、commit user 或真实数据文件名。

## 性能验证与结论边界

局部基准已完成 **24 个 JVM、168 个测量批次、144 项加载类来源及摘要核对**。两侧使用冻结依赖和对应源码独立编译的四个热类；候选热类与最终 clean 产物字节一致。环境为 JDK 8u202、固定单 CPU 亲和性、1 GiB 堆，p=1/4、4/32 字段、3 forks；每 JVM 8 次预热、7 个测量批次、每批 100,000 行。原百万行计划已标记中止，最终数据全部重新运行，未混入中止样本。

先取每个 fork 七批的中位数，再取三个 fork 的中位数：

| p | 字段数 | elapsed ns/row：基线 → 候选 | elapsed 变化 | CPU 变化 | allocated bytes/row：两侧相同 |
| ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 4 | 364.668 → 378.553 | +3.81% | +5.23% | 1016.034 |
| 1 | 32 | 5485.017 → 5483.911 | −0.02% | −0.57% | 7368.000 |
| 4 | 4 | 863.472 → 856.514 | −0.81% | −0.08% | 2142.469 |
| 4 | 32 | 12032.324 → 12338.641 | +2.55% | +2.55% | 14168.000 |

各场景测量线程分配量差值均为 0 bytes/row，满足原工具的 elapsed 中位数不超过基线 105%、分配量不增加的数值门槛。但两个 p=1 场景候选 fork 相对极差分别为 **23.22% 和 15.38%**，超过原定 15% 提示值，不能宣称稳定无回退或加速；p=4、32 字段实测耗时增加约 2.55%，按原样保留。

完整局部结果见 `/tmp/paimon-recovery-micro-summary.md`；原始 CSV SHA-256 为 `663bf1162743df61979e8dd16b27c8f7b953069230afbd5b3dc5a86ae412d321`。已核对每行计算路径没有新增算法操作，但观测差异不能归零。该基准不包含真实 MySQL、Flink shuffle、Checkpoint 或 Paimon 存储，p=4 仍由单线程依次调用四个 Writer；分配量不代表整个作业内存。

真实 MySQL 旧/新适配器 A/B 已完成 **8 次独立运行：p=4 三对、p=1 一对，全部 PASS**。每次均使用当前 Connector，比较旧/新 Adapter 拓扑，不是完整历史镜像与候选镜像的对比；性能运行关闭故障注入和 Savepoint，与前述恢复正确性验收分开。

主指标为 `initialRowsVisibleMillis`，从作业提交到两张表合计 40,000 条初始记录在目标完整可见，包含作业启动和提交等待。首条记录后的 rows/s 起点不同，仅作辅助指标，不用于替换主指标。

| 并行度 | 配对轮次 | 旧 Adapter：提交至全量可见 ms | 新 Adapter：提交至全量可见 ms |
| ---: | ---: | ---: | ---: |
| 4 | 1 | 38610 | 39172 |
| 4 | 2 | 38477 | 34395 |
| 4 | 3 | 38585 | 37272 |
| 1 | 1 | 77555 | 73654 |

p=4 三次中位数为 **38585 → 37272 ms（−3.40%）**；旧值范围 38477–38610 ms，新值范围 34395–39172 ms，范围重叠且新侧波动明显，不能声称稳定提速。p=1 只有一对，观测为 77555 → 73654 ms（−5.03%），仅作探索数据，不作稳定结论。

8 次运行的 `run.properties` 均确认两表各 20,000 条完整主键与字段及 I/U/D 通过。对应 `cleanup-result.properties` 均为 `tablesRemoved=2`、`verifiedAbsent=true`，16 张本轮隔离源表已清理并确认不存在。原始证据位于 `/tmp/paimon-mysql-acceptance/performance-20260912/`。

本实验说明指定源与本地目标条件下两种拓扑的观测差异，不推断 PolarDB、Kubernetes 或对象存储环境的端到端 QPS；全量可见耗时也不能代替纯恢复补交耗时。

### Snapshot split 参数筛选

另外完成两次当前候选的参数筛选：p=4、`snapshot.fetch.size=1024`、两张源表各 20,000 行、宽表 payload=3000、窄表 payload=16，其余条件不变，将 `snapshot.split.size` 从 1024 改为 8192。两次完整主键、字段和 I/U/D 验证均 PASS，清理记录均为 `tablesRemoved=2`、`verifiedAbsent=true`。

| split | 样本数 | 提交至全量可见 s | 全量可见中位数 s | 提交至首条记录 s | 最大 RSS KiB |
| ---: | ---: | --- | ---: | --- | --- |
| 1024 | 3 | 39.172 / 34.395 / 37.272 | 37.272 | 9.177 / 10.054 / 10.113 | 未采集同口径基线 |
| 8192 | 2 | 23.866 / 25.982 | 24.924 | 13.254 / 12.840 | 1,623,428 / 1,457,136 |

该组数据中，全量可见耗时中位数下降 **33.13%**，同时首条记录出现更晚。8192 两轮为连续筛选，并非随机配对实验，样本不足以确认稳定收益；也没有测量实际 split 或 backfill 数量，不能宣称它们减少八倍。证据汇总见 `/tmp/paimon-mysql-acceptance/all-performance-summary.json`。

RSS 来自 GNU `time -v` 对整个 Maven 命令及其子进程口径记录的最大值，不是 Flink Writer 独立内存，也不是各进程 RSS 之和。缺少 split=1024 的同口径基线，不能据此判断内存没有退化。

Connector 当前默认 split 为 **8096，非 8192**；上述对照的 1024 是测试显式值，原业务运行实例的 Source 参数尚未确认。本轮未修改默认参数，不将这次筛选描述为恢复适配器本身的加速或生产提速。

## 后续删除兼容层的条件

未来升级 Flink 时，只有官方适配路径同时满足以下条件，才可考虑删除 `FlinkRecoverySink` 和定制 Global Committer：

- 初始化后晚到的完整历史提交无需新 Checkpoint 先行即可处理，且仍具备完成证明、生产者完整性和历史顺序保护。
- 真实原版本恢复点可读取，算子身份匹配；旧迁移声明可以退出而不会丢失状态；部分状态缩容和 terminal 状态的边界有明确处理。
- 四 Writer、多表、最新空边界、故障重试、显式暂停恢复、后续全量主键字段及 I/U/D 回归全部通过。
- 官方路径调用的恢复提交语义仍能发布必要的空恢复标记，使 Writer 安全重开，且常态空提交没有持续元数据退化。
- 同一冻结输入与制品来源的性能对照满足验收要求。

单纯提高 Flink 版本不能替代上述证明；满足条件后再删除兼容代码和过时说明，避免长期保留两套协议。

## 运行交付边界

源码、正确性、局部基准、8 次 A/B、两次 split 筛选及恢复补交计时已有上述证据；仅增加计时后的最终单测和 clean package 也已通过。最终四个模块的范围内格式检查及完整 diff 检查也已通过；源码提交和候选制品身份由独立交付记录绑定。5,604 ms 表示暂停期间变化追赶总耗时，1.569 s 表示本地合成场景的恢复提交委托耗时，两者不能互相替代。

本记录完成 Issue #118 的源码和本地验证范围。镜像发布、Jenkins 构建、实际 Pod 制品与业务任务恢复分别留存运行验收证据，本地结果不等同于这些步骤已经完成。

## 真实 MySQL 验收复现

测试默认不执行外库连接。准备权限为 `0600` 的临时 properties 文件，包含 `host`、`port`、`database`、`username`、`password`；通过文件路径参数传入，日志按敏感证据保存。

```bash
umask 077
mvn -f seatunnel-connectors-v2/connector-paimon/pom.xml \
  -Dskip.spotless=true -Dtest=PaimonMySqlCdcRecoveryIT \
  -Dcf.mysql.acceptance.config=/path/to/private-mysql.properties \
  -Dcf.mysql.acceptance.output=/path/to/new-acceptance-output \
  -Dcf.mysql.acceptance.parallelism=4 \
  -Dcf.mysql.acceptance.rows=20000 \
  -Dcf.mysql.acceptance.payload=3000 test
```

默认开启故障注入与显式 Savepoint 恢复；性能对照显式设置 `cf.mysql.acceptance.fail=false`、`cf.mysql.acceptance.savepoint=false`。旧适配器对照设置 `cf.mysql.acceptance.legacy=true`；分片筛选设置 `cf.mysql.acceptance.splitSize=8192`。这些都是测试开关，没有修改运行时默认值。

每轮生成唯一命名合成源表、独立 warehouse 和 `run.properties`。测试保留源表和 `cleanup.sql` 供核验；清理时只执行该轮清单中的两张表并检查不存在，不使用通配删除。此次完整实库验收及性能各轮的源表已按清单清理，临时连接文件已移除、专用 SSH 隧道已关闭；独立目标与恢复证据保留供审计。
