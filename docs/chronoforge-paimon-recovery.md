# Paimon 历史提交恢复（ChronoForge Issue #118）

本次修复面向 SeaTunnel 2.3.13、Flink 1.18.1、Paimon 1.1.1 的固定分桶路由任务。
生产代码仍按 Flink 1.15.3 的既有接口编译。

## 恢复责任

Writer 在历史提交确认前不能继续写入同一个 bucket。原有安全守卫保留：
历史数据提交确认后重新打开 TableWrite，超时或无法确认就失败。
路由任务恢复统一由 Global Committer 发布历史数据，包括缩容后的单 Writer。
单 Writer 也不能自行补交最新 Writer state，因为全局状态中可能还存在更旧的提交边界。
错误发生在提交协调时机：本地 Committer 恢复时补发消息晚于 Global Committer 的
状态初始化；旧全局算子需要下一次 Checkpoint 完成才能处理这些消息，Writer 却正等它提交。

`FlinkRecoverySink` 复用 Flink 的 Writer、状态和本地 Committer，只替换末端全局算子。
`FlinkRecoveryGlobalCommitterOperator` 在消息到达和 Checkpoint 完成时处理同一队列：

1. Flink 已确认该 Checkpoint 完成；恢复时恢复点本身就是完成证明。
2. 同一边界必须收齐所有上游 Committer 的声明和消息，包含零消息的声明。
3. 所有消息合并后只提交一次，成功后推进并持久化已提交边界。
4. 未收齐的旧边界阻止新边界越过；提交失败或要求重试时保留状态并使任务失败。

因此恢复补交不依赖下一次 Checkpoint，也不允许单个 Writer 的片段提前提交。
恢复批次使用已有 `SinkAggregatedCommitter.restoreCommit`，多表提交器继续向每个子提交器
传递恢复语义。Paimon 仅在这条路径设置 `ignoreEmptyCommit(false)`，使最新空边界也有
可验证的 snapshot；常态 `commit` 继续忽略空提交。即使最新 Writer state 全空、全局
仍有更旧的非空数据，Writer 也要等到最新恢复边界确认后再重新打开 TableWrite。
普通运行仍要求 Checkpoint 完成证明，不依赖仅凭消息到达就提交的新版 Flink 行为。
Flink Checkpoint ID 与 SeaTunnel/Paimon 内部 commitIdentifier 独立编号，排障时不可混用。

## 旧状态兼容

保持原 `Global Committer` 拓扑名称、并行度 1、maxParallelism 1 和
`streaming_committer_raw_states` 状态描述符。测试比较新旧完整 JobGraph 的算子身份；
不通过 `allowNonRestoredState` 丢弃未匹配状态。

全局状态 v3 保存每个边界的原 Writer 数量、Writer 身份、声明消息数、已收到的消息和
已提交边界。它独立于原有 Paimon Writer routing state v2，不改变 Writer 的序列化格式。

Flink 1.15/1.18 的全局状态 v2 不保存原 Writer 身份、数量及原始期望消息数。
迁移仅支持本项目 Paimon 每 Writer、每边界恰好一个 `CommitWrapper` 的旧布局；
Paimon 内部没有数据增量时，仍会产生这个外层包装。读取时保留所有原分段：

- 全部旧 Writer 分段各包含一个未排出的消息，才可整批补交。
- 全部分段均已排出一个消息，才可认定为已提交残留。
- 缺段、混合已排出/待提交、失败、未知版本或不符布局均拒绝恢复。

**首次从非空旧全局状态升级必须保持原 Writer 并行度，并核对原 Checkpoint 的算子并行度。**
从所选恢复点的 metadata 核实 Checkpoint ID 和 Writer 算子的原并行度后，提供一次性声明：

```hocon
env {
  paimon.legacy-state {
    checkpoint-id = 42
    writer-parallelism = 4
  }
}
```

这是运维根据原 metadata 提供的迁移声明，代码不会把当前并行度当成历史证据。
声明的 Checkpoint ID 必须匹配实际恢复点，旧并行度必须匹配本次 Writer 并行度，
否则拒绝非空 v2 状态，且不会先提交其中的片段。完全空 v2 状态和 v3 不需要该声明。
需要缩容至单 Writer 时，先以原并行度完成升级恢复并产生 v3 Savepoint，再按已有
Writer routing state v2 的缩容协议操作。v3 保留历史生产者数量，不拿新并行度代替旧数量。
含部分全局提交消息的状态先按原并行度恢复、完成提交后再暂停；Flink 的中间 Committer
在缩容时会重写原生产者身份，本次不通过猜测身份支持这种部分状态直接缩容。
持续 CDC 恢复使用未结束输入的 Checkpoint 或不 drain 的 Savepoint。
若全局状态的已提交边界或待提交批次包含 Flink 结束输入标识 `Long.MAX_VALUE`，
初始化会在补交任何批次前拒绝恢复，要求选择不 drain 的 Savepoint 或终止前的 Checkpoint。
该标识不能当作普通 Checkpoint 编号，否则可能永久过滤后续正常消息；本次不猜测编号映射，
不支持 drain/terminal 状态恢复为持续作业。正常有界输入的最终提交仍由 `endInput` 完成。
这补充了 [固定分桶路由文档](chronoforge-paimon-routing.md) 的恢复边界。

## 维护边界

特殊逻辑集中在 Flink 适配层；Paimon 每行路由与 `convertForWriter` 热路径没有新增远端检查。
旧格式解码器只是迁移入口，不能扩展为猜测任意 Flink 状态布局的通用工具。
后续升级 Flink 时，先用本次故障恢复、旧状态迁移、算子身份和全字段读回用例验证官方
提交路径，再决定删除该兼容层，避免长期维护两套提交协议。

测试分别覆盖真实算子故障恢复、改造前 Checkpoint 恢复、新状态协议边界和算子身份。
真实 MySQL 验收需显式提供受限权限的临时连接文件，并使用独立测试表及独立 Paimon warehouse。
本地 MiniCluster/文件系统吞吐不等于 PolarDB、Kubernetes 或对象存储环境的端到端吞吐。
构建、镜像发布和原业务任务恢复需要分别留存证据。

实测矩阵、性能限制与复现命令见 [改造验证记录](chronoforge-paimon-recovery-validation.md)。
