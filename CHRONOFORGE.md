# dev-liwei 运行时源码基线

## 分支用途

`201811510411lw/seatunnel` 是 `apache/seatunnel` 的 fork。
`dev-liwei` 是个人长期运行时维护分支，基于 SeaTunnel `2.3.13`（`8c9d47f5e`），
用于 ChronoForge 的 SeaTunnel on Flink `1.18.1` 执行环境。
日常修复和功能在同一分支按独立提交累积，不要求每个功能永久保留一个新分支。
已有 `dev`、`runtime/2.3.13-chronoforge` 和历史修复分支保留，不重写或删除。

## 2026-09-09 镜像对应关系

本次归集依据已发布镜像的构建清单，而不是把所有历史分支直接合并：

```text
image=prod-mirror-cn-beijing.cr.volces.com/metadata/seatunnel-flink
published.digest=sha256:daad1c87eb305784ba96212a2f876949dc23ea1e5fa033fdf83333addd4b4b7b
base.digest=sha256:6f7344229399f3a745b1a145ee2735366db2fde57f2c6cc344ce556a85d464f7
source.base=8c9d47f5e
flink.version=1.18.1
java.version=8
```

源码已直接应用 ChronoForge 的六个 `third-party/seatunnel/patches/*.patch`，
并纳入原来仅由镜像构建脚本复制的 `SQLMultiCatalogRoutingTest`。
检出本分支后无需再次 `git am` 这些补丁。

| 功能 | 源码或回归入口 |
| --- | --- |
| Paimon writer state 序列化 | `PaimonSink#getWriterStateSerializer` |
| Flink / Paimon Checkpoint 事务边界 | `FlinkSinkWriterCheckpointBoundaryTest`、`PaimonCheckpointCommitTest` |
| Checkpoint 确认的 CDC 阶段与 Gauge | `CdcProgressPhaseTracker`、`FlinkMetricContext` |
| 有界表级 Snapshot 进度 | `CdcSnapshotProgress`、`CdcTableProgress` |
| 有界 Split 明细与恢复后的进度发布 | `CdcSplitProgress`、`IncrementalSourceEnumeratorCdcProgressTest` |
| 恢复及扩缩容后重建增量分片分配 | `IncrementalSplitReportEvent`、`IncrementalSourceEnumeratorRescaleTest` |
| 多表 SQL 未匹配透传和连续 SQL 字段顺序 | `IdentityFlatMapTransform`、`SQLMultiCatalogRoutingTest` |

Transform 修复已在上游基线中，本分支补齐回归测试，不从 `3.0.0-SNAPSHOT` 移植实现。
平台侧 CDC 参数保护、运行观测 API 和前端属于 ChronoForge 仓库，不属于 SeaTunnel fork。

### 历史分支不等同于当前镜像源码

旧运行分支还包含 `5c5345923`（主键作为 Snapshot 分片列）、`ef3fc338c`
（CDC Base shaded Commons Lang3 打包）和 `45c501770`（旧 Dispatcher 构造函数 ABI）。
当前覆盖层从 `8c9d47f5e` 加六个补丁重新生成配套 CDC Base / MySQL CDC，
不直接以该旧运行分支构建。因此本次不额外合入这三个提交，以免改变当前镜像的基线。
它们仍在旧分支中，不能把“历史上推送过”当成“当前覆盖 JAR 中仍然包含”。
后续如需恢复其中某项能力，应在 `dev-liwei` 上单独集成、测试并发布新镜像。

### 构建产物核对

以下是已发布覆盖层清单中的预期 SHA-256：

```text
seatunnel-flink-15-starter.jar=9e9cb8248fe313f6b9cff37df68e29097e810dd875c8599fcf4dca649f2bdc95
connector-cdc-base-2.3.13.jar=fad553a77f388041ebb4ec7f44aee4e9a7c16a5ef78379b4e701f91bb13019c5
connector-cdc-mysql-2.3.13.jar=1fe54845acd8d8d1e73e190b4f2f3a662d6c7417befaf89b26b7684b8b3036c0
seatunnel-transforms-v2.jar=5222cfc973c07bd41920459cde4d9eb4e5260b04f13cf414abc0509ae66bc87a
```

MySQL CDC 和 Transform JAR 必须经 ChronoForge 的
`third-party/seatunnel/runtime-r10/normalize-jar.py` 归一化后再比较摘要；
锁定环境为 Python `3.12.3`、zlib `1.3`。
本机使用 `/home/lsym005226/project/starrocks-cleanup-audit/ai-env/bin/python`。

Paimon connector 沿用基础镜像中的二进制，SHA-256 为
`2d6be950b5e66ad41e57c4c105063d104dbde73e7443143e2c3f359541c3ae51`。
本分支包含对应功能源码和 Checkpoint 回归，但不宣称独立重建的 Paimon JAR
与这个既有二进制逐字节一致；不得未经验证替换它或其他基础镜像组件。

## 本地编译和回归

在分支根目录执行以下命令；需要已安装兼容的同版本 Maven 依赖。
这里只覆盖当前镜像涉及的模块，不代表整个 Apache SeaTunnel 项目全量测试。
在曾构建其他分支的目录中执行时，应先清理这些模块的旧 `target`，防止残留 class 混入。

```bash
export JAVA_HOME=/home/lsym005226/project/jdk1.8.0_202
export PATH="$JAVA_HOME/bin:$PATH"

mvn -f seatunnel-api/pom.xml -Dskip.spotless=true -DskipTests install
mvn -f seatunnel-core/seatunnel-flink-starter/seatunnel-flink-starter-common/pom.xml \
  -Dskip.spotless=true -DskipTests install
mvn -f seatunnel-connectors-v2/connector-cdc/connector-cdc-base/pom.xml \
  -Dskip.spotless=true test install
mvn -f seatunnel-connectors-v2/connector-cdc/connector-cdc-mysql/pom.xml \
  -Dskip.spotless=true -DskipTests package
mvn -f seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-common/pom.xml \
  -Dskip.spotless=true -Dtest=FlinkSinkWriterCheckpointBoundaryTest test install
mvn -f seatunnel-connectors-v2/connector-paimon/pom.xml \
  -Dskip.spotless=true -Dtest=PaimonCheckpointCommitTest test
mvn -f seatunnel-core/seatunnel-flink-starter/seatunnel-flink-15-starter/pom.xml \
  -Dskip.spotless=true -DskipTests package
mvn -f seatunnel-transforms-v2/pom.xml \
  -Dskip.spotless=true -Dmaven.test.skip=false -DskipTests=false package
```

## 后续维护和发布边界

Paimon 固定分桶路由修复与旧状态迁移约束见 `docs/chronoforge-paimon-routing.md`。
该修复属于后续源码变更，不包含在下面记录的历史发布镜像及 414 项验证结果中。

本次归集验证（2026-09-09）：上述八个 Maven 步骤均成功，CDC Base 44、Flink
Checkpoint 1、Paimon Checkpoint 2、Transform 367 项测试全部通过，共 414 项。
清理受影响模块旧产物后重建，四个覆盖 JAR 的 SHA-256 全部与上面的发布清单一致。
Python 归一化使用前述固定解释器。验证日志保存在执行机器的
`/tmp/seatunnel-dev-liwei-20260909-build.log` 和
`/tmp/seatunnel-dev-liwei-20260909-artifact-check.log`，临时日志不是 Git 交付物。
本次未运行整个 SeaTunnel 根 reactor 的全量构建、Spotless 或端到端测试，未触发 Jenkins。

1. SeaTunnel 运行时变更统一落在 `dev-liwei`，保留独立、可回退的提交。
2. 不直接合并上游 `dev`；升级 SeaTunnel、Flink 或 connector 需要独立兼容性验证。
3. 每次发布记录准确的源码提交 SHA、测试结果、产物摘要和镜像 digest，不能只记录可变分支名或 `v1`。
4. 当前镜像装配、lock 和补丁消费入口仍在 ChronoForge 的 `third-party/seatunnel/runtime-r10/`。
   本次仅归集、验证并发布 fork 分支，不修改该仓库既有工作区，也不切换其构建来源。
   后续改为直接消费本分支时，必须锁定具体 SHA 并停止重复应用已经集成的六个补丁。
5. 推送源码不触发本次重新构建或部署镜像；Argo 同步由用户执行。
   Pod imageID、成功 Checkpoint 和 Paimon 数据验收仍是独立步骤。
