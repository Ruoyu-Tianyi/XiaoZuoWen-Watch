# XiaoZuoWen Watch · 小作文观察

卖方观点的本地增量处理与研究看板。保存获授权的原文，识别具体 A/H 股公司的明确看多、看空观点，匹配标准证券代码，保留证据、推荐方及审批记录，再统计已发布观点的热度与历史提及。

**专有软件，保留所有权利。** 本仓库用于项目展示与版本管理；查看源码不等于获得使用或商业授权。Tianyi Zhou（GitHub：Ruoyu-Tianyi）保留原创部分后续商业化、收费服务与独立授权的权利。详见 [LICENSE](LICENSE) 和 [第三方权利说明](THIRD_PARTY_NOTICES.md)。

## 当前能力与状态

- 原文分页抓取、版本留存、去重及断点恢复；带图片或文件的主题只保存原文和元数据。
- 模型分类与观点抽取、证券主表匹配、逐条 AI 复核，明确观点自动接受，疑点留给人工。
- 同篇同公司合并；支持 SH/SZ/BJ/HK 和 AH 双代码；保留目标价币种与精确原文证据。
- 单批调用数、输入字符及耗时预算，上海自然日跨批次调用与字符上限；缓存复用、异常隔离及只读验收。
- 最近 24 小时、72 小时、7 天热度前 50，历史标的搜索及审核管理。

当前是人工触发、触发后小批自动闭环、可配置自动发布的工作流。每组完成识别与独立复核后，若开启自动发布，仅把置信度与本地门禁通过的观点发布到看板；疑点转人工，不增加第三轮重复模型调用。再次触发可在每日预算内接续预算暂停的近期批次。每天定时触发、系统自启动和自动主表刷新尚未启用；不能把后台运行等同于无人值守。详见 [工作流说明](docs/PROJECT_WORKFLOW.md)。

## 本地准备

以下操作说明供权利人和获得书面授权的使用者参考。需要 Node.js 22.13+（建议 24 LTS）、Git，以及各数据与模型服务的独立授权。

```powershell
npm ci
Copy-Item config/workflow.example.json config/workflow.json
```

编辑本机 config/workflow.json，填写自己获授权的 group_id。配置模板不含真实星球 ID、个人路径或凭据；本机配置不会提交。知识星球 CLI 应先完成自己的登录。模型配置须使用自己的账号确实可用的模型 ID，模板中的 ID 不代表服务可用性保证。

Tushare 凭据通过 TUSHARE_TOKEN、TUSHARE_TOKEN_FILE 或 TUSHARE_CREDENTIAL_ZIP 提供，不写入源码。运行模型会向所配置的服务发送正文，须先取得相应内容处理授权。

```powershell
npm run workflow:init
npm run workflow:sync-securities
npm start
```

看板地址：http://127.0.0.1:4173/ 。数据库初始化后尚无已发布观点时，空看板是正常状态。

## 增量处理与验证

```powershell
npm run workflow:recent
npm run workflow:run -- --hours=24 --limit=6 --batch-size=6 --max-calls=4 --max-chars=80000 --max-minutes=12
npm run workflow:automation-status
npm run workflow:validate-batch -- --batch=1
npm test
```

验收命令中的批次 ID 需替换为实际 ID。暂停后可使用 `npm run workflow:run -- --resume=批次ID`；沿用累计预算，耗尽的上限不会自动重置。AI 接受不等于发布，热度看板读取已发布观点。设置本机 `automation.auto_publish=true` 或显式 `--auto-publish` 开启自动发布；默认公开模板仍关闭。自动发布要求观点与审核置信度不低于0.95、无主题级疑点及有效证据，备份后事务内复查并保存审计。人工决定及其他批次旧记录不自动发布。

`workflow:recent` 默认抓取最近7天、最多10篇一组，每次运行新增调用上限20次、输入300,000字符、60分钟；每日所有批次共用40次调用及600,000字符硬上限。再次触发时只自动接续准备或预算暂停批次，不自动重试结构异常或人工停止。账号精确美元成本不可由 CLI 调用计数推算。

经明确授权可为指定批次追加 `--unlimited-calls` 取消批次与每日调用次数上限，配合 `--concurrency=4` 最多四路处理提速。`--keyed-output` 将文章编号设为结构化输出的必填键，防止重复编号挤掉其他文章。调用继续记账，字符、时长、账号额度和错误保护仍独立生效；自动发布保持备份和事务门禁，其他批次默认预算不受影响。详见工作流文档的运行示例。

## 仓库同步范围

同步 app、scripts、通用配置、配置模板及公开说明。data、reports、数据库、备份、日志、node_modules、个人配置、私有审核样本及历史本地笔记均留在本机并被忽略。源码中的测试使用构造数据，不连接真实星球或真实模型。

package.json 中保留了旧版回溯命令；它们可能依赖未公开的本机数据或审核样本，不属于当前日常入口。后续正常同步使用 Git 提交与推送，不通过强制推送覆盖远程历史。
