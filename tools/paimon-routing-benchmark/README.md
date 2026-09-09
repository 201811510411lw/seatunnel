# Paimon 路由 / Writer 热路径基准

固定基线：`180c2d730409bfc7a89670423c6b6f84c377ed91`。同一份 Java main 只编译一次，
分别加载真实基线与候选编译类。没有复制基线算法、JMH、新增依赖、JUnit 时间断言或生产开关。

## 测量边界

- 使用模块现有 test classpath，参考 `PaimonCheckpointCommitTest`，真实创建本地
  Paimon catalog、分区表、`PaimonSink` 和 `PaimonSinkWriter`。
- 只在 harness 内通过反射关闭并替换 Writer 的私有 `tableWrite`。轻量 `TableWrite`
  消费每次写入的全部字段与 RowKind，不保存 invocation、不使用 Mockito 热路径。
  其他方法一律拒绝，避免未来生产路径变化时静默失真。
- 计时循环调用真实 upstream `SinkWriteRouting.route`，再调用选中 Writer 的真实 `write`。
  upstream 与每个 Writer 使用独立 routing 实例。Writer 自身归属校验、行转换、安全上下文
  都保留；表创建、Writer 初始化、原始 TableWrite 关闭与正确性准备均不计时。
- **这不是端到端性能测试**：不含真实 TableWrite 内部路由、存储、网络、Flink shuffle、
  Checkpoint、compaction、恢复，也不模拟并发执行。`p=2` 表示路由到两个 Writer，
  两者仍在一个线程依次调用；不能据此宣称双并行度吞吐量或生产验收通过。

## 场景与正确性

固定四场景：`p=1/2 × 4/32 fields`。表 `bucket=8`，`field_1` 为七个字符串分区，
主键为 `field_0 + field_1`，自定义 bucket key 为 `field_0`。其余列交替使用
`decimal(18,2)` 与含中文字符串，约 20% nullable；输入轮换四种 RowKind。
默认预生成 4096 行，所有批次从相同位置循环复用，不在计时区生成数据。

计时前直接构造 Paimon `GenericRow`，用 Paimon 自己的 `FixedBucketRowKeyExtractor`
与 `ChannelComputer.select` 校验每个输入行的 owner，不借用 Connector 的转换结果作为 oracle。
预计算每个 Writer 的行数与顺序敏感 checksum；每批消费全部输出字段并在计时后断言，
最终写入 volatile blackhole 和 CSV，避免结果未消费导致消除。要求所有 Writer 实际收到行。
CSV 还输出预生成输入的 `route_checksum`，分析时须核对基线/候选同场景 checksum 一致。
这些检查不替代行为回归，hash 也不是无碰撞的逐字段断言。

## 准备编译类

1. 将固定 SHA 基线与候选源码分别构建，保留独立、不可变的产物。不要边编译边测量。
2. 各提供一份 UTF-8 单行 test classpath 文件，使用显式绝对路径和冒号分隔，不使用 `*`。
   包括各自匹配的 Connector、API 以及其他已编译模块和测试依赖；模块 `target/classes`
   应排在同名依赖 JAR 前。不要让基线/候选指向同一份会被覆盖的 Maven SNAPSHOT JAR。
3. 可以从相应构建的 Surefire XML 中提取 `java.class.path`，但须核对每个路径真实来源，
   冻结可变本地 Maven JAR；旧报告里的 classpath 不是当前 SHA 的构建证据。
4. 脚本不 checkout、不运行 Maven、不新增生产依赖。输入 SHA 是调用者提供的构建归属声明，
   脚本不能仅凭 classpath 自动证明源码 SHA。执行者须保存源码状态、构建日志和依赖摘要。
   候选如含未提交修改，传 HEAD 并另存 diff/产物证据，不得把 HEAD 称为完整候选源码身份。

以下示例假设已导出 `/tmp/paimon-perf-classpath.txt`，并在基线 worktree
`/tmp/paimon-perf-baseline-180c2d730` 完成编译。下面仅生成 classpath，不执行编译或基准：

```bash
/home/lsym005226/project/starrocks-cleanup-audit/ai-env/bin/python - <<'PY'
from pathlib import Path

candidate = Path("/tmp/paimon-perf-classpath.txt").read_text().strip()
candidate = ":".join(entry for entry in candidate.split(":") if entry)
current = "/home/lsym005226/project/seatunnel/seatunnel-connectors-v2/connector-paimon/target/"
baseline = "/tmp/paimon-perf-baseline-180c2d730/seatunnel-connectors-v2/connector-paimon/target/"
assert current + "test-classes" in candidate and current + "classes" in candidate
Path("/tmp/paimon-perf-baseline.classpath").write_text(candidate.replace(current, baseline) + "\n")
Path("/tmp/paimon-perf-candidate.classpath").write_text(candidate + "\n")
PY
```

这只替换 Connector 的两个 target 前缀；若 API/其他模块也变化，必须另外冻结与替换，
不能把未替换的共享依赖默认视为正确。父 POM 可能跳过 `dependency:build-classpath`，
不要因该目标返回成功就假定依赖文件已更新；可从刚完成构建的 Surefire XML 的
`./properties/property[@name='java.class.path']` 读取 `value`，核对其模块前缀后再使用。

另一种方式是在对应 worktree 中只执行依赖目标，显式关闭 `connectors-v2` 父 POM 的跳过项：

```bash
mvn -f seatunnel-connectors-v2/connector-paimon/pom.xml \
  -De2e.dependency.skip=false dependency:build-classpath \
  -Dmdep.outputFile=/absolute/dependencies.txt
```

该命令不附带 compile/test/package 生命周期；输出是依赖，不包含本模块编译目录，仍须将
对应 worktree 的 `target/test-classes:target/classes` 加到前面，不能加载陈旧 installed
Connector 替代实际编译 classes。

## 运行（等待 CPU 独占窗口）

```bash
cd /home/lsym005226/project/seatunnel
JAVA_HOME=/home/lsym005226/project/jdk1.8.0_202 \
CPUSET=2 FORKS=5 WARMUPS=8 BATCHES=7 ITERATIONS=1000000 ROWS=4096 HEAP=1g \
bash tools/paimon-routing-benchmark/run.sh \
  /absolute/baseline.classpath /absolute/candidate.classpath \
  "${CANDIDATE_SHA:?请先设置候选完整40位SHA}" /absolute/new-output-directory
```

输出目录必须不存在，父目录必须存在。`CPUSET` 可省略；设置时应由主代理选择实际允许且
适用的 CPU，taskset 只是亲和性，不会自动隔离其他进程。不要同时运行 Maven、其他基准
或高负载任务。脚本使用同用户共享 `flock` 防重复启动，但不能阻止其他用户或绕过脚本的负载。
所有 JVM 串行执行；奇数 fork 为 baseline→candidate，偶数反向，按场景配对交替。
每个场景/版本/fork 都是新 JVM，固定 JDK8、相同 Xms/Xmx、ParallelGC；拒绝隐式注入 JVM 参数。
默认 `JIT_MODE=blocking-c2` 增加 `-Xbatch -XX:-TieredCompilation -XX:ParallelGCThreads=1`，
避免单 CPU 亲和性下后台分层编译和大量 GC 线程干扰；这仅是基准控制变量，不是生产调优建议。
`JIT_MODE=tiered` 保留 JDK 默认编译及 ParallelGC 线程策略，用于另一次独立实验。
两个版本始终使用同一模式；不同模式的原始数据不得混合汇总，必须分别报告。
运行期间不要修改 `run.sh` 或 Java harness；需要调整配置时等待当前脚本完全退出，
或预先冻结一份独立脚本再启动。失败或截断的样本不能用于验收，补测须重跑对应配对。
默认总计 40 个 JVM，运行前应预留足够的 CPU 时间。

需要仅验证可启动时，可设置 `FORKS=1 WARMUPS=1 BATCHES=1 ITERATIONS=4096`；
该结果只用于功能冒烟，不能用于性能判断。javac 只编译这个 main，不会编译生产模块。

## 输出与解释

- `results.csv`：合并所有测量批次；原始 `*-fork*.csv` 各有表头，预热不输出。
- `ns_per_row`：单线程 wall-clock ns / 输入行；包括路由、Writer 和完整行消费成本。
- `allocated_bytes_per_row`：读取本地 JDK8 `com.sun.management.ThreadMXBean` 的
  `getThreadAllocatedBytes` 当前测量线程堆分配近似累计计数，用批次差值除以输入行数。
  它不是 RSS、堆存活量或全作业所有线程分配，不含其他线程，不能据此宣称整作业内存下降。
  不支持分配计数的 JVM 直接失败，不把缺失计数当作零。
- `checksum`、`route_checksum`：先验证同场景跨版本/批次一致，再讨论性能。
- `environment.txt`、`order.txt`、`harness.sha256`：环境、执行顺序、SHA 声明与同一 harness 摘要。
  各 JVM `.log` 记录实际加载核心类的来源路径和 class SHA-256，便于发现 classpath 污染。
  JVM 日志与 CSV 分离。所有临时仓库保存在输出目录 `tmp/`，不自动删除，供异常排查。

使用以下命令汇总：

```bash
/home/lsym005226/project/starrocks-cleanup-audit/ai-env/bin/python \
  tools/paimon-routing-benchmark/summarize.py /absolute/output/results.csv --forks 5 --batches 7
PYTHONDONTWRITEBYTECODE=1 /home/lsym005226/project/starrocks-cleanup-audit/ai-env/bin/python \
  -m unittest discover -s tools/paimon-routing-benchmark -p 'test_*.py'
```

统计时先求每个 fork 内各批次中位数，再求各 fork 中位数，按四场景分别比较，
同时报告离散程度与分配量，不能混合窄/宽表或并行度抵消回退。建议每场景候选延迟中位数
不超过基线的 105%，分配 bytes/row 不高于基线，正式测量至少三个独立 forks；
接近阈值、方差大或置信不足属于灰区，需独占 CPU 增加 forks 复测，
不能冒称通过。按需分析 CSV 时，默认解释器使用
`/home/lsym005226/project/starrocks-cleanup-audit/ai-env/bin/python`；汇总脚本仅使用标准库。

汇总会拒绝缺失场景、重复/缺失批次、混用输入规模/版本、非有限数值及 checksum 不一致。
`--forks`、`--batches` 必须与运行计划一致，不能根据残缺 CSV 降低预期数量来通过检查；
checksum 必须为有效 signed 64-bit 整数，合法零值不会被拒绝。
`within_threshold` 只是逐场景数值门槛；`needs_noise_review=true` 表示独立 fork 的耗时
中位数极差超过整体中位数的 15%，必须检查原始数据，不能只读取通过标志作最终验收。

工具仅存在于本目录，不修改生产代码、POM、既有测试或 ChronoForge；不代表提交、推送、
镜像发布、上线或 Issue 关闭。
