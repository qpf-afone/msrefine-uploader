# MSRefine Windows 采集与上传

持续扫描 MSRefine 结果目录中的 `INFO_Summary.txt`，解析后写入本机 SQLite 队列，再通过 HTTPS 上传。收到并核验服务器的提交回执后，任务标记为 `uploaded`。

## 准备

安装 Python 3.10 或以上版本。采集端使用 Python 标准库。在 Windows 下载或克隆本仓库，将 PowerShell 的工作目录切换到仓库根目录。

## 初次登记

将示例中的 `D:\MSRefineResults` 换成实际要上传的结果目录。程序会递归扫描该目录下已有和后续出现的汇总文件。

```powershell
py -3 --version
py -3 -m msrefine_parser watch 'D:\MSRefineResults' --state-dir '.\state' --scan-once
py -3 -m msrefine_parser queue --state-dir '.\state' --status all --limit 1
```

将队列输出的 `collector_id` 交给服务器管理员，由管理员将其绑定到对应仪器并提供 `collector.token`。通过私密渠道接收凭据，将文件存放在仓库根目录并限制读取权限。后续启动沿用同一个状态目录，以保留采集端身份和待上传任务。状态目录放在本机可写磁盘。

## 启动采集和上传

初次配置时，复制 [上传配置模板](examples/upload-config.example.json)。

```powershell
Copy-Item '.\examples\upload-config.example.json' '.\upload-config.local.json'
```

将 `upload-config.local.json` 中的 `endpoint` 改为管理员提供的 HTTPS 上传地址，包含 `/api/ingest/results` 路径。使用内部 CA 的服务器时，将管理员提供的 CA 证书存为 `server-ca.crt`，在 JSON 中添加字段 `"ca_file": "server-ca.crt"`。证书相对路径以配置文件所在目录为准。保存配置为 UTF-8 无 BOM。

```powershell
$env:MSREFINE_UPLOAD_TOKEN = (Get-Content -LiteralPath '.\collector.token' -Raw -ErrorAction Stop).Trim()
py -3 -m msrefine_parser watch 'D:\MSRefineResults' --state-dir '.\state' --upload-config '.\upload-config.local.json'
```

默认每 5 秒扫描一次，文件稳定至少 30 秒并满足连续观察次数后才会入队。看到 `state\uploader.log` 中的“上传已确认”，或者在另一个终端使用 `queue --status all` 查询到 `uploaded`，即可确认本次上传。断网时任务保留在本机，程序会自动重试。按 `Ctrl+C` 停止前台程序。

## 开机运行

前台验证成功后，用 Windows 任务计划程序创建“启动时”触发的任务。“起始于”填写仓库根目录。“程序或脚本”填写 `powershell.exe`。用 `py -3 -c "import sys; print(sys.executable)"` 获取 Python 的实际路径，替换下面的示例路径和结果目录，再将整行填入“添加参数”。

```text
-NoProfile -NonInteractive -Command "$ErrorActionPreference = 'Stop'; $env:MSREFINE_UPLOAD_TOKEN = (Get-Content -Raw -LiteralPath '.\collector.token').Trim(); & 'C:\path\to\python.exe' -m msrefine_parser watch 'D:\MSRefineResults' --state-dir '.\state' --upload-config '.\upload-config.local.json'; exit $LASTEXITCODE"
```

任务账户需要能读取程序、结果目录和凭据，写入状态目录并访问 HTTPS。需要开机后无人登录也能运行时，选择“不管用户是否登录都要运行”，保存时使用该 Windows 账号的密码。启用失败重启，取消运行时长限制，并选择已有实例运行时跳过新实例。先手动运行任务，再重启 Windows 检查任务状态和日志。

## 代码目录

- `msrefine_parser/` 包含命令行、TXT 解析、文件监测、SQLite 队列和上传器。
- `msrefine_protocol/` 包含版本化上传协议和回执校验。
