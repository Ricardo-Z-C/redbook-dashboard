# Streamlit Cloud 一键部署指南（给产品经理版）

这个方案会把页面部署到云端，**不再依赖你的本地电脑持续开机**。

---

## 0. 你会得到什么

- 一个固定的公网链接（例如 `https://xxx.streamlit.app`）
- 你关机后页面仍可访问
- 以后只要更新 GitHub 代码，线上会自动更新

---

## 1. 先理解一个关键事实（避免踩坑）

当前项目里有两类能力：

1) **看板类能力**（老板看板、执行中心、配置管理等）  
2) **本地依赖能力**（浏览器扫码登录、部分爬取与本地 MCP 协同）

部署到 Streamlit Cloud 后：

- 看板类能力可正常在线使用
- 本地依赖能力可能不可用（因为云端没有你本机浏览器环境）

如果你后续想把“爬取+执行”也完全云端化，需要单独做“云端 worker + 持久化数据库”方案（可后续再做）。

---

## 2. 创建 GitHub 仓库（只放 `MediaCrawler` 目录）

建议新建一个仓库，比如：`redbook-dashboard`

只上传 `MediaCrawler` 目录内容，不要上传整个大工作区。

---

## 3. 上传代码到 GitHub（最简单方式）

你可以直接用 GitHub 网页上传，或者用 Cursor/Git 工具上传。  
只要最终保证仓库根目录里有这些文件即可：

- `dashboard.py`
- `requirements.txt`
- `runtime.txt`
- `config/` 目录
- `database/` 目录（可为空，不建议上传真实业务数据）

---

## 4. 在 Streamlit Cloud 发布

1. 打开 [https://share.streamlit.io/](https://share.streamlit.io/) 并登录 GitHub  
2. 点击 **New app**  
3. Repository 选择你的 `redbook-dashboard`  
4. Branch 选 `main`  
5. Main file path 填：`dashboard.py`  
6. 点击 **Deploy**

首次部署需要 2-10 分钟安装依赖。

---

## 5. 配置密钥（可选但推荐）

如果你需要老板看板里的 AI 能力，在 Streamlit Cloud 的 App 设置里添加 Secrets：

- `LIST_FILTER_API_KEY`
- `LIST_FILTER_API_BASE`
- `LIST_FILTER_MODEL`
- `AGENT_REPORT_API_KEY`
- `AGENT_REPORT_API_BASE`
- `AGENT_REPORT_MODEL`

不配置也能打开页面，只是 AI 相关功能会受限。

---

## 6. 常见问题

### Q1: 部署成功但某些“爬取/评论执行”功能不可用？

这是预期现象，原因是这些功能依赖本地浏览器/MCP 服务，不是纯前端看板。

### Q2: 数据为什么重启后丢失？

默认是本地 SQLite 文件，云端实例重启后可能重置。  
要长期稳定存储，需要把数据库迁移到云数据库（如 Supabase Postgres）。

### Q3: 我只要老板看板稳定在线，怎么做最省事？

先用当前方案上线看板，再按需补“云数据库”。

