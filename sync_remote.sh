#!/bin/bash

# Agoda Data Pipeline - Git同步脚本
# 用于将本地项目推送到远程GitHub仓库

set -e  # 遇到错误时退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查Git是否已初始化
check_git_repo() {
    if [ ! -d ".git" ]; then
        log_error "当前目录不是Git仓库"
        read -p "是否初始化Git仓库? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            git init
            log_success "Git仓库初始化完成"
        else
            log_error "操作取消"
            exit 1
        fi
    fi
}

# 检查远程仓库
check_remote() {
    if ! git remote get-url origin >/dev/null 2>&1; then
        log_warning "未配置远程仓库"
        read -p "请输入GitHub仓库URL: " repo_url
        if [ -n "$repo_url" ]; then
            git remote add origin "$repo_url"
            log_success "远程仓库配置完成: $repo_url"
        else
            log_error "未提供仓库URL，操作取消"
            exit 1
        fi
    else
        remote_url=$(git remote get-url origin)
        log_info "远程仓库: $remote_url"
    fi
}

# 检查工作区状态
check_working_directory() {
    if [ -n "$(git status --porcelain)" ]; then
        log_info "检测到未提交的更改:"
        git status --short
        return 0
    else
        log_info "工作区干净，没有未提交的更改"
        return 1
    fi
}

# 添加文件到暂存区
add_files() {
    log_info "添加文件到暂存区..."
    
    # 确保重要文件被添加
    important_files=(
        "README.md"
        "requirements.txt"
        "pg_room.py"
        "example_usage.py"
        "config/"
    )
    
    for file in "${important_files[@]}"; do
        if [ -e "$file" ]; then
            git add "$file"
            log_info "已添加: $file"
        fi
    done
    
    # 添加其他修改的文件
    git add .
    
    # 排除不需要的文件
    if [ -f ".gitignore" ]; then
        log_info "使用.gitignore规则过滤文件"
    else
        # 创建基本的.gitignore
        cat > .gitignore << EOF
# Python
__pycache__/
*.py[cod]
*\$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# Logs
*.log

# Database
*.db
*.sqlite3

# Environment
.env
.venv
env/
venv/

# Config (if contains sensitive data)
# config/config.yml
EOF
        git add .gitignore
        log_success "创建并添加了.gitignore文件"
    fi
}

# 提交更改
commit_changes() {
    # 检查是否有暂存的更改
    if [ -z "$(git diff --cached --name-only)" ]; then
        log_warning "没有暂存的更改需要提交"
        return 1
    fi
    
    # 显示将要提交的更改
    log_info "将要提交的更改:"
    git diff --cached --name-status
    
    # 获取提交信息
    if [ -n "$1" ]; then
        commit_message="$1"
    else
        echo
        read -p "请输入提交信息 (默认: 'Update project files'): " commit_message
        commit_message=${commit_message:-"Update project files"}
    fi
    
    # 执行提交
    git commit -m "$commit_message"
    log_success "提交完成: $commit_message"
    return 0
}

# 推送到远程仓库
push_to_remote() {
    local branch=$(git branch --show-current)
    log_info "当前分支: $branch"
    
    # 检查远程分支是否存在
    if git ls-remote --heads origin "$branch" | grep -q "$branch"; then
        log_info "推送到远程分支: $branch"
        git push origin "$branch"
    else
        log_info "首次推送分支: $branch"
        git push -u origin "$branch"
    fi
    
    log_success "推送完成"
}

# 主函数
main() {
    log_info "开始同步到远程仓库..."
    
    # 检查Git仓库
    check_git_repo
    
    # 检查远程仓库配置
    check_remote
    
    # 检查工作区状态
    if check_working_directory; then
        # 有未提交的更改
        echo
        read -p "是否继续提交并推送? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "操作取消"
            exit 0
        fi
        
        # 添加文件
        add_files
        
        # 提交更改
        if commit_changes "$1"; then
            # 推送到远程
            push_to_remote
        fi
    else
        # 没有未提交的更改，直接推送
        log_info "尝试推送现有提交..."
        if git log --oneline -1 >/dev/null 2>&1; then
            push_to_remote
        else
            log_warning "没有提交记录，请先进行一些更改"
        fi
    fi
    
    log_success "同步完成！"
    
    # 显示仓库状态
    echo
    log_info "当前仓库状态:"
    git status --short
    
    # 显示最近的提交
    echo
    log_info "最近的提交:"
    git log --oneline -5
}

# 帮助信息
show_help() {
    echo "用法: $0 [选项] [提交信息]"
    echo
    echo "选项:"
    echo "  -h, --help     显示帮助信息"
    echo "  -f, --force    强制推送（谨慎使用）"
    echo
    echo "示例:"
    echo "  $0                           # 交互式同步"
    echo "  $0 \"Add new features\"         # 使用指定的提交信息"
    echo "  $0 -f \"Force update\"        # 强制推送"
}

# 解析命令行参数
FORCE_PUSH=false
COMMIT_MSG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -f|--force)
            FORCE_PUSH=true
            shift
            ;;
        *)
            COMMIT_MSG="$1"
            shift
            ;;
    esac
done

# 如果是强制推送模式
if [ "$FORCE_PUSH" = true ]; then
    log_warning "强制推送模式已启用"
    read -p "确定要强制推送吗? 这可能会覆盖远程更改 (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        git push --force-with-lease
        log_success "强制推送完成"
        exit 0
    else
        log_info "强制推送取消"
        exit 0
    fi
fi

# 执行主函数
main "$COMMIT_MSG"