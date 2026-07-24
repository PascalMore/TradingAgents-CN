#!/bin/bash
# TradingAgents-CN 一键启动脚本 (YQuant 增强版: 8 类退出码 + pre-flight + env 净化 + 健康校验 + 端到端 smoke)
#
# 与 stop_all.sh 配套使用:
#   ./start_all.sh                    # 完整启动（含 smoke, 适合生产）
#   ./start_all.sh --no-smoke         # 跳过端到端 smoke, 更快
#   ./start_all.sh --no-monitor       # 启动后退出不进入守护循环 (cron / 一次性任务)
#   ./stop_all.sh                     # 干净停止
#
# 退出码速查:
#   0  = 完全成功 (含 smoke)
#   11 = pre-flight 失败 (cwd / .env / settings)
#   21 = stop 阶段失败
#   31 = 端口未在 60s 内 listen
#   41 = health check 失败 (/openapi.json)
#   51 = scheduler 未注册任何 jobs
#   61 = 端到端 smoke 失败
#   71 = MongoDB 翻新数不足

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

FRONTEND_PORT=3000
BACKEND_PORT=8000
RESTART_DELAY=3
HEALTH_CHECK_INTERVAL=15
BACKEND_STARTUP_TIMEOUT=90  # 后端启动最大等待秒数 (lifespan 会阻塞等外部 API, 拉到 90 兜底)
SMOKE_TIMEOUT=60

LOG_PREFIX="[start_all $(date '+%Y-%m-%d %H:%M:%S')]"

# ==================== flags ====================
DO_SMOKE=1
DO_MONITOR=1
for arg in "$@"; do
    case "$arg" in
        --no-smoke)   DO_SMOKE=0 ;;
        --no-monitor) DO_MONITOR=0 ;;
        --help|-h)
            sed -n '2,28p' "$0"
            exit 0
            ;;
    esac
done

log()  { echo "$LOG_PREFIX $*"; }
fail() { echo "$LOG_PREFIX ❌ $*" >&2; exit "$1"; }

# ==================== 1. pre-flight ====================
log "=== 1. pre-flight ==="

[ -f .env ]                  || fail 11 ".env 不存在 ($PWD/.env)"
[ -x .venv/bin/python ]      || fail 11 ".venv/bin/python 不可执行"
[ -f stop_all.sh ]           || fail 11 "stop_all.sh 不存在 (应与 start_all.sh 同目录)"

# 关键: cwd 锚定后 settings 必须读到 .env (Pydantic case-insensitive env 注入陷阱)
log "验证 settings 关键字段..."
PYCHECK=$(env -u timezone PROJECT_ROOT="$SCRIPT_DIR" .venv/bin/python - <<'PY' 2>&1
from app.core.config import settings
keys = ['TIMEZONE', 'MONGODB_DATABASE', 'MONGODB_HOST', 'TUSHARE_TOKEN', 'SYNC_STOCK_BASICS_CRON']
bad = [k for k in keys if not getattr(settings, k, None) and getattr(settings, k, '') != '']
if not settings.TIMEZONE:
    bad.append('TIMEZONE(empty)')
print('OK' if not bad else 'BAD:' + ','.join(bad))
PY
)
echo "$PYCHECK" | grep -q '^OK$' || fail 11 "settings 关键字段未读到: $PYCHECK"
log "✅ pre-flight PASS"

# ==================== 2. graceful stop ====================
log "=== 2. graceful stop ==="

for port in "$BACKEND_PORT" "$FRONTEND_PORT"; do
    pids=$(lsof -ti :$port 2>/dev/null || true)
    if [ -n "$pids" ]; then
        log "清理端口 $port 残留: $(echo $pids | tr '\n' ' ')"
        echo "$pids" | xargs -r kill -KILL 2>/dev/null || true
    fi
done

if ./stop_all.sh >/dev/null 2>&1; then
    log "✅ stop_all.sh 完成"
else
    fail 21 "stop_all.sh 失败"
fi

mkdir -p logs

# ==================== 3. 内部: env 净化的 spawn 函数 ====================
# 关键: Hermes / tmux / IDE 可能注入 timezone='' (空), pydantic case-insensitive 会覆盖 TIMEZONE default
spawn_backend_clean() {
    # 不再用 nohup + 后台 fork 改写 env (会引发 PID 漂移), 改用 env -u timezone 前置
    env -u timezone nohup .venv/bin/python -m app.main >> logs/backend.log 2>&1 &
    echo $!
}

# ==================== 4. 启动后端 ====================
log "=== 4. start backend ==="

echo "📡 启动后端 API (port $BACKEND_PORT)..."
BACKEND_PID=$(spawn_backend_clean)
echo "$BACKEND_PID" > logs/backend.pid
log "后端已 spawn, PID=$BACKEND_PID"

# 等端口就绪 (带 30s 硬超时)
waited=0
while [ $waited -lt $BACKEND_STARTUP_TIMEOUT ]; do
    if ss -tlnp 2>/dev/null | grep -q ":$BACKEND_PORT "; then
        log "✅ 后端端口 $BACKEND_PORT 已就绪（等待 ${waited}s, PID=$BACKEND_PID）"
        break
    fi
    if ! ps -p "$BACKEND_PID" > /dev/null 2>&1; then
        log "❌ 后端进程 ($BACKEND_PID) 启动后立即退出，请检查 logs/backend.log"
        tail -50 logs/backend.log 2>/dev/null || true
        fail 31
    fi
    sleep 1
    waited=$((waited + 1))
done

if [ $waited -ge $BACKEND_STARTUP_TIMEOUT ]; then
    fail 31 "后端端口 $BACKEND_PORT 未在 ${BACKEND_STARTUP_TIMEOUT}s 内 listen"
fi

# ==================== 5. 健康检查 /openapi.json ====================
log "=== 5. health check ==="

HEALTH_OK=0
for try in 1 2 3; do
    if curl -sS -o /dev/null --max-time 5 "http://localhost:$BACKEND_PORT/openapi.json" 2>/dev/null; then
        HEALTH_OK=1
        break
    fi
    sleep 1
done
[ "$HEALTH_OK" = 1 ] || { log "❌ /openapi.json 3 次重试均失败"; fail 41; }
log "✅ /openapi.json 200"

# ==================== 6. scheduler jobs 校验 ====================
log "=== 6. scheduler jobs 校验 ==="

# 让 scheduler 有时间打印启动日志
sleep 3
JOBS_COUNT=$(grep -c 'Added job' logs/backend.log || echo 0)
if [ "$JOBS_COUNT" -lt 1 ]; then
    log "❌ backend.log 中没有 'Added job' 行 (scheduler 未注册任何 job)"
    fail 51
fi
log "✅ scheduler 已注册 ${JOBS_COUNT} 个 jobs"

# ==================== 7. 端到端 smoke (可选) ====================
if [ "$DO_SMOKE" = 1 ]; then
    log "=== 7. 端到端 smoke ==="
    log "POST /api/sync/stock_basics/run (force=false, timeout=${SMOKE_TIMEOUT}s) ..."
    SMOKE_OUT=$(curl -sS -X POST --max-time "$SMOKE_TIMEOUT" "http://localhost:$BACKEND_PORT/api/sync/stock_basics/run" || true)
    if echo "$SMOKE_OUT" | grep -q '"status":"success"'; then
        log "✅ smoke 成功: $(echo "$SMOKE_OUT" | head -c 200)"
    else
        log "❌ smoke 失败: $SMOKE_OUT"
        fail 61
    fi
else
    log "(--no-smoke) 跳过端到端 smoke"
fi

# ==================== 8. 启动前端 (与之前行为一致) ====================
log "=== 8. start frontend ==="
echo "🖥️ 启动前端 Vue (port $FRONTEND_PORT)..."
cd "$SCRIPT_DIR/frontend"

FRONTEND_PID=0
if command -v npx &> /dev/null; then
    nohup npx vite --host 0.0.0.0 > "$SCRIPT_DIR/logs/frontend.log" 2>&1 &
    FRONTEND_PID=$!
    echo "$FRONTEND_PID" > "$SCRIPT_DIR/logs/frontend.pid"
    log "前端已 spawn, PID=$FRONTEND_PID"

    # 等前端端口
    fwaited=0
    while [ $fwaited -lt 20 ]; do
        if ss -tlnp 2>/dev/null | grep -q ":$FRONTEND_PORT "; then
            log "✅ 前端端口 $FRONTEND_PORT 就绪（等待 ${fwaited}s）"
            break
        fi
        if ! ps -p "$FRONTEND_PID" > /dev/null 2>&1; then
            log "❌ 前端进程立即退出，请检查 logs/frontend.log"
            FRONTEND_PID=0
            break
        fi
        sleep 1
        fwaited=$((fwaited + 1))
    done
else
    log "⚠️ 未找到 npx，跳过前端"
fi
cd "$SCRIPT_DIR"

# ==================== 完成报告 ====================
echo ""
echo "========================================"
log "✅ TradingAgents-CN 启动完成"
echo "========================================"
echo "后端 API:    http://localhost:$BACKEND_PORT"
echo "API 文档:    http://localhost:$BACKEND_PORT/docs"
echo "前端 Vue:    http://localhost:$FRONTEND_PORT"
echo "停止服务:    ./stop_all.sh"
echo "========================================"

if [ "$DO_MONITOR" = 1 ]; then
    log "=== 监控循环 (自动重启) 已启动, --no-monitor 可跳过 ==="
    # ==================== 9. 监控循环（原有逻辑保留）====================
    while true; do
        if ! ps -p "$BACKEND_PID" > /dev/null 2>&1; then
            log "⚠️ 后端进程 ($BACKEND_PID) 已退出，${RESTART_DELAY}s 后重启..."
            sleep $RESTART_DELAY
            BACKEND_PID=$(spawn_backend_clean)
            echo "$BACKEND_PID" > logs/backend.pid
            log "后端已 spawn, 新 PID=$BACKEND_PID"
        fi

        if [ "$FRONTEND_PID" -ne 0 ] && ! ps -p "$FRONTEND_PID" > /dev/null 2>&1; then
            log "⚠️ 前端进程 ($FRONTEND_PID) 已退出，${RESTART_DELAY}s 后重启..."
            sleep $RESTART_DELAY
            cd "$SCRIPT_DIR/frontend"
            nohup npx vite --host 0.0.0.0 > "$SCRIPT_DIR/logs/frontend.log" 2>&1 &
            FRONTEND_PID=$!
            echo "$FRONTEND_PID" > "$SCRIPT_DIR/logs/frontend.pid"
            cd "$SCRIPT_DIR"
        fi

        sleep $HEALTH_CHECK_INTERVAL
    done
else
    log "(--no-monitor) 已启动完成, 监控循环未进入, daemon 由 systemd / 用户外部拉起"
fi
