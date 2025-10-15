# Agoda Data Pipeline

一个高性能的Agoda数据管道系统，支持多线程数据插入、连接池管理和配置驱动的架构设计。

## 🚀 核心特性

- **多线程数据插入**: 支持并发数据处理，显著提升插入性能
- **连接池管理**: 智能数据库连接池，优化资源利用
- **配置驱动**: 基于YAML的配置管理，支持多环境部署
- **模块化架构**: 采用领域驱动设计(DDD)的分层架构
- **类型安全**: 完整的类型注解，提升代码质量
- **向后兼容**: 保持单线程模式的完全兼容性

## 📁 项目结构

```
agoda_data_pipe/
├── config/                    # 配置模块
│   ├── __init__.py            # 包初始化文件
│   ├── config.yml             # 主配置文件
│   ├── config_manager.py      # 配置管理器
│   └── sql_config.py          # 数据库配置加载器
├── pg_room.py                 # 核心数据管道模块
├── example_usage.py           # 使用示例
├── sync_remote.sh             # Git同步脚本 (Linux/macOS)
├── sync_remote.ps1            # Git同步脚本 (PowerShell)
├── sync_remote.bat            # Git同步脚本 (Windows批处理)
├── requirements.txt           # 项目依赖
└── README.md                  # 项目文档
```

## 🛠️ 安装与配置

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置数据库

编辑 `config/config.yml` 文件，配置数据库连接信息：

```yaml
# 环境配置
environment:
  current: "development"  # production 或 development

# 数据库配置
database:
  table_name: "agoda_source_data"
  
  # 开发环境配置
  development:
    host: "localhost"
    dbname: "room_match"
    user: "postgres"
    password: "your_password"
    port: 5432
    client_encoding: "UTF8"

# 应用配置
app:
  batch_size: 100
  log_level: "INFO"
  
  # 连接池配置
  connection_pool:
    min_connections: 2
    max_connections: 10
    connection_timeout: 30
    idle_timeout: 300
    
  # 多线程配置
  threading:
    max_workers: 4
    chunk_size: 1000
    enable_threading: false  # 默认关闭多线程
```

## 🎯 快速开始

### 基本使用

```python
from pg_room import AgodaDataPipeline

# 示例数据
room_data = [
    {
        "hotel_id": "12345",
        "room_type": "Deluxe Room",
        "price": 150.00,
        "availability": True
    },
    # 更多数据...
]

# 使用上下文管理器
with AgodaDataPipeline() as pipeline:
    # 创建表
    pipeline.create_table()
    
    # 插入数据（自动选择单线程或多线程模式）
    success = pipeline.insert_data(room_data)
    
    if success:
        print("数据插入成功")
```

### 多线程模式

```python
from pg_room import AgodaDataPipeline

# 启用多线程模式（在config.yml中设置enable_threading: true）
with AgodaDataPipeline() as pipeline:
    # 强制使用多线程插入
    success = pipeline.insert_data_multi_thread(room_data)
```

### 单线程模式

```python
from pg_room import AgodaDataPipeline

with AgodaDataPipeline() as pipeline:
    # 强制使用单线程插入
    success = pipeline.insert_data_single_thread(room_data)
```

## 🏗️ 架构设计

### 核心组件

1. **ConfigManager**: 配置管理器，负责加载和管理YAML配置
2. **ConnectionPoolManager**: 连接池管理器，管理数据库连接池
3. **DatabaseManager**: 数据库管理器，处理单连接模式
4. **ThreadedDataManager**: 多线程数据管理器，处理并发数据插入
5. **DataManager**: 单线程数据管理器，处理传统数据插入
6. **TableManager**: 表管理器，负责表的创建和维护
7. **AgodaDataPipeline**: 主数据管道类，统一接口

### 设计原则

- **配置驱动**: 所有配置通过YAML文件管理
- **开闭原则**: 对扩展开放，对修改关闭
- **委托模式**: 使用委托而非继承
- **类型安全**: 完整的类型注解
- **模块化**: 功能模块化，便于维护

## ⚡ 性能特性

### 多线程优势

- **并发处理**: 支持多线程并发数据插入
- **智能分块**: 自动将大数据集分割为合适的块
- **连接池**: 高效的数据库连接复用
- **错误处理**: 完善的异常处理和回滚机制

### 配置参数

- `max_workers`: 最大工作线程数（默认4）
- `chunk_size`: 数据块大小（默认1000）
- `min_connections`: 最小连接数（默认2）
- `max_connections`: 最大连接数（默认10）

## 🔧 命令行使用

```bash
# 运行主程序
python -m pg_room

# 查看配置示例
python example_usage.py
```

## 📊 日志记录

系统提供详细的日志记录，包括：

- 数据库连接状态
- 数据插入进度
- 错误和异常信息
- 性能统计信息

日志级别可在配置文件中调整：

```yaml
app:
  log_level: "INFO"  # DEBUG, INFO, WARNING, ERROR
```

## 🛡️ 错误处理

- **连接失败**: 自动重试和连接池管理
- **数据验证**: 插入前的数据格式验证
- **事务回滚**: 失败时自动回滚事务
- **异常日志**: 详细的错误日志记录

## 🔄 环境切换

支持多环境配置，通过修改 `config.yml` 中的 `environment.current` 值：

```yaml
environment:
  current: "production"  # 切换到生产环境
```

## 📈 扩展性

- **插件架构**: 易于添加新的数据源和目标
- **配置扩展**: 支持自定义配置参数
- **处理器扩展**: 可添加自定义数据处理器
- **监控集成**: 支持集成监控和告警系统

## 🔄 Git同步脚本

项目提供了便捷的Git同步脚本，用于将代码推送到远程GitHub仓库：

### Windows用户

#### 方法1: 使用批处理文件（推荐）

```cmd
# 交互式同步
sync_remote.bat

# 使用指定的提交信息
sync_remote.bat "Add new features"

# 强制推送（谨慎使用）
sync_remote.bat -f "Force update"

# 显示帮助信息
sync_remote.bat -h
```

#### 方法2: 直接使用PowerShell脚本

```powershell
# 交互式同步
.\sync_remote.ps1

# 使用指定的提交信息
.\sync_remote.ps1 -CommitMessage "Add new features"

# 强制推送（谨慎使用）
.\sync_remote.ps1 -Force -CommitMessage "Force update"

# 显示帮助信息
.\sync_remote.ps1 -Help
```

### Linux/macOS用户

```bash
# 给脚本添加执行权限
chmod +x sync_remote.sh

# 交互式同步
./sync_remote.sh

# 使用指定的提交信息
./sync_remote.sh "Add new features"

# 强制推送（谨慎使用）
./sync_remote.sh -f "Force update"

# 显示帮助信息
./sync_remote.sh -h
```

### 脚本功能特性

- **自动检测**: 自动检测Git仓库状态和远程配置
- **智能添加**: 自动添加重要项目文件到暂存区
- **交互确认**: 提供用户友好的交互式确认
- **错误处理**: 完善的错误处理和状态检查
- **彩色输出**: 清晰的彩色日志输出
- **自动.gitignore**: 自动创建Python项目的.gitignore文件
- **分支管理**: 智能处理首次推送和后续推送
- **强制推送保护**: 安全的强制推送选项

### 首次使用

1. 确保已安装Git并配置了GitHub账户
2. 在GitHub上创建远程仓库
3. 运行同步脚本，按提示输入仓库URL
4. 脚本会自动配置远程仓库并完成首次推送

## 🤝 贡献指南

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🆘 支持

如果您遇到问题或有疑问，请：

1. 查看文档和示例代码
2. 检查配置文件设置
3. 查看日志输出
4. 提交 Issue 描述问题

---

**注意**: 请确保在生产环境中使用前充分测试配置和连接设置。