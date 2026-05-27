<template>
  <div class="stock-pool">
    <div class="toolbar">
      <div>
        <h1>股票池</h1>
        <p>自动规则引擎按信号强度、产品共识和拥挤度推进生命周期。</p>
      </div>
      <div class="actions">
        <el-button :icon="Refresh" @click="loadAll">刷新</el-button>
        <el-button type="primary" plain :icon="Top" @click="runPromotion(true)">预览升区</el-button>
        <el-button type="primary" :icon="Top" @click="runPromotion(false)">执行升区</el-button>
        <el-button type="warning" plain :icon="Bottom" @click="runDemotion(true)">预览降区</el-button>
        <el-button type="warning" :icon="Bottom" @click="runDemotion(false)">执行降区</el-button>
      </div>
    </div>

    <el-row :gutter="12" class="zone-grid" v-loading="loading">
      <el-col v-for="zone in zones" :key="zone.key" :xs="24" :sm="12" :lg="6">
        <el-card class="zone-card" :body-style="{ padding: '12px' }" shadow="never">
          <template #header>
            <div class="zone-head">
              <div>
                <span class="zone-title">{{ zone.label }}</span>
                <span class="zone-sub">{{ zone.key }}</span>
              </div>
              <el-tag size="small" :type="zone.type">{{ zoneItems(zone.key).length }} / {{ capacity.zones?.[zone.key] || 0 }}</el-tag>
            </div>
          </template>

          <div v-if="zoneItems(zone.key).length" class="stock-list">
            <div
              v-for="item in zoneItems(zone.key)"
              :key="item.id"
              class="stock-row"
              :class="{ matched: isRuleMatched(item.id) }"
              @click="showAudit(item)"
            >
              <div class="row-left">
                <span class="stock-dot" :class="sourceColor(item.source)" />
                <div class="stock-info">
                  <span class="stock-name">{{ item.stock_name || item.wind_code }}</span>
                  <span class="stock-code">{{ item.wind_code }}</span>
                </div>
              </div>
              <div class="row-right">
                <el-tag size="small" :type="sourceTag(item.source).type" class="source-tag">{{ sourceTag(item.source).label }}</el-tag>
                <el-tag size="small" :type="crowdingTagType(item)" class="crowding-tag">{{ crowdingLabel(item) }}</el-tag>
                <div class="metrics">
                  <span>贝叶斯 {{ pct(metric(item, 'bayesian')) }}</span>
                  <span>共识 {{ pct(metric(item, 'consensus')) }}</span>
                  <span>产品 {{ metric(item, 'product_count') }}</span>
                </div>
              </div>
            </div>
          </div>
          <el-empty v-else description="暂无股票" :image-size="48" />
        </el-card>
      </el-col>
    </el-row>

    <el-drawer v-model="previewDrawer" title="规则执行预览" size="520px">
      <el-alert
        v-if="lastRun"
        :type="lastRun.dry_run ? 'info' : 'success'"
        :closable="false"
        :title="runSummary"
        show-icon
      />
      <el-table :data="lastRun?.items || []" size="small" style="margin-top: 12px">
        <el-table-column prop="wind_code" label="代码" width="110" />
        <el-table-column prop="stock_name" label="名称" width="110" />
        <el-table-column label="区间" min-width="150">
          <template #default="{ row }">{{ row.from_zone }} -> {{ row.target_zone }}</template>
        </el-table-column>
        <el-table-column prop="rule" label="规则" min-width="150" />
      </el-table>
    </el-drawer>

    <el-dialog v-model="auditDialog" title="审计历史" width="720px">
      <el-table :data="audits" size="small" v-loading="auditLoading">
        <el-table-column prop="created_at" label="时间" min-width="170">
          <template #default="{ row }">{{ formatTime(row.created_at) }}</template>
        </el-table-column>
        <el-table-column prop="action" label="动作" width="140" />
        <el-table-column prop="actor" label="操作者" width="150" />
        <el-table-column label="变化" min-width="160">
          <template #default="{ row }">
            {{ row.before?.pool_zone || '-' }} -> {{ row.after?.pool_zone || '-' }}
          </template>
        </el-table-column>
      </el-table>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { Bottom, Refresh, Top } from '@element-plus/icons-vue'
import { ApiClient } from '@/api/request'

defineOptions({ name: 'StockPool' })

type ZoneKey = 'SCAN' | 'WATCH' | 'CANDIDATE' | 'CONVICTION'

interface StockPoolItem {
  id: string
  stock_code: string
  wind_code: string
  stock_name: string
  pool_zone: ZoneKey
  source: string
  source_detail?: string
  entry_reason?: Record<string, any>
  [key: string]: any
}

interface RuleRun {
  action: string
  dry_run: boolean
  matched: number
  changed: number
  skipped: number
  items: Array<Record<string, any>>
}

const zones: Array<{ key: ZoneKey; label: string; type: 'info' | 'warning' | 'success' | 'danger' }> = [
  { key: 'SCAN', label: '扫描区', type: 'info' },
  { key: 'WATCH', label: '观察区', type: 'warning' },
  { key: 'CANDIDATE', label: '候选区', type: 'success' },
  { key: 'CONVICTION', label: '高确信区', type: 'danger' }
]

const items = ref<StockPoolItem[]>([])
const loading = ref(false)
const previewDrawer = ref(false)
const lastRun = ref<RuleRun | null>(null)
const audits = ref<any[]>([])
const auditDialog = ref(false)
const auditLoading = ref(false)
const capacity = reactive<{ zones: Record<string, number>; total: number }>({ zones: {}, total: 0 })

const runSummary = computed(() => {
  if (!lastRun.value) return ''
  const count = lastRun.value.dry_run ? lastRun.value.matched : lastRun.value.changed
  return `${lastRun.value.action === 'promote' ? '升区' : '降区'}${lastRun.value.dry_run ? '预览' : '执行'}：${count} 条命中，${lastRun.value.skipped} 条跳过`
})

const loadAll = async () => {
  loading.value = true
  try {
    const [poolRes, capacityRes] = await Promise.all([
      ApiClient.get<{ items: StockPoolItem[] }>('/api/portfolio/stock-pool', { limit: 200 }),
      ApiClient.get<{ zones: Record<string, number>; total: number }>('/api/portfolio/stock-pool/capacity')
    ])
    items.value = poolRes.data.items || []
    capacity.zones = capacityRes.data.zones || {}
    capacity.total = capacityRes.data.total || 0
  } finally {
    loading.value = false
  }
}

const runPromotion = async (dryRun: boolean) => {
  const res = await ApiClient.post<RuleRun>('/api/portfolio/stock-pool/trigger-promote', { dry_run: dryRun }, { showLoading: true })
  lastRun.value = res.data
  previewDrawer.value = true
  if (!dryRun) await loadAll()
}

const runDemotion = async (dryRun: boolean) => {
  const res = await ApiClient.post<RuleRun>('/api/portfolio/stock-pool/trigger-demote', { dry_run: dryRun }, { showLoading: true })
  lastRun.value = res.data
  previewDrawer.value = true
  if (!dryRun) await loadAll()
}

const zoneItems = (zone: ZoneKey) => items.value.filter((item) => item.pool_zone === zone)

const sourceTag = (source: string) => {
  const map: Record<string, { label: string; type: 'success' | 'primary' | 'warning' | 'danger' | 'info' }> = {
    argus: { label: '🟢 Argus', type: 'success' },
    smart_money_institution: { label: '🔵 机构SmartMoney', type: 'primary' },
    smart_money_retail: { label: '🟡 牛散SmartMoney', type: 'warning' },
    smart_money_kol: { label: '🟠 KOLSmartMoney', type: 'danger' },
    manual: { label: '手动', type: 'info' }
  }
  return map[source] || { label: source || '未知', type: 'info' }
}

const metric = (item: StockPoolItem, key: string) => {
  const reason = item.entry_reason || {}
  const metadata = reason.metadata || {}
  const sources = [item, reason, metadata]
  const read = (keys: string[], fallback: any) => {
    for (const source of sources) {
      for (const name of keys) {
        if (source?.[name] !== undefined && source?.[name] !== null) return source[name]
      }
    }
    return fallback
  }
  if (key === 'bayesian') return Number(read(['bayesian_score', 'bayesian', 'score', 'confidence'], 0))
  if (key === 'consensus') return Number(read(['consensus_confidence', 'consensus_score', 'consensus'], 0))
  if (key === 'product_count') {
    const products = read(['contributing_products', 'products'], [])
    return Number(read(['contributing_products_count', 'product_count', 'products_count'], Array.isArray(products) ? products.length : 0))
  }
  if (key === 'crowding_level') return String(read(['crowding_level', 'crowding'], 'LOW')).toUpperCase()
  return ''
}

const pct = (value: unknown) => `${(Number(value || 0) * 100).toFixed(0)}%`

const warningLevel = (item: StockPoolItem) => {
  const level = String(metric(item, 'crowding_level'))
  return level === 'DANGER' || level === 'HIGH' ? `拥挤${level}` : ''
}

const sourceColor = (source: string) => {
  const map: Record<string, string> = {
    argus: 'dot-green',
    smart_money_institution: 'dot-blue',
    smart_money_retail: 'dot-yellow',
    smart_money_kol: 'dot-orange',
    manual: 'dot-gray',
  }
  return map[source] || 'dot-gray'
}

const crowdingTagType = (item: StockPoolItem) => {
  const level = String(metric(item, 'crowding_level'))
  if (level === 'DANGER' || level === 'HIGH') return 'danger'
  if (level === 'MEDIUM') return 'warning'
  return 'info'
}

const crowdingLabel = (item: StockPoolItem) => {
  const map: Record<string, string> = { LOW: '低拥挤', MEDIUM: '中拥挤', HIGH: '高拥挤', DANGER: '危险' }
  const level = String(metric(item, 'crowding_level'))
  return map[level] || level
}

const isRuleMatched = (id: string) => Boolean(lastRun.value?.items?.some((item) => item.id === id))

const ruleTooltip = (id: string) => {
  const item = lastRun.value?.items?.find((row) => row.id === id)
  if (!item) return ''
  return Object.entries(item.thresholds || {})
    .map(([key, value]) => `${key}: ${value}`)
    .join(' / ')
}

const openDetail = (item: StockPoolItem) => {
  ElMessage.info(`${item.stock_name || item.wind_code} 当前位于 ${item.pool_zone}`)
}

const showAudit = async (item: StockPoolItem) => {
  auditDialog.value = true
  auditLoading.value = true
  try {
    const res = await ApiClient.get<{ items: any[] }>(`/api/portfolio/stock-pool/audit/${item.id}`)
    audits.value = res.data.items || []
  } finally {
    auditLoading.value = false
  }
}

const formatTime = (value?: string) => {
  if (!value) return '-'
  return new Date(value).toLocaleString()
}

onMounted(loadAll)
</script>

<style lang="scss" scoped>
.stock-pool {
  .toolbar {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 16px;
    margin-bottom: 16px;

    h1 {
      margin: 0 0 6px;
      font-size: 22px;
      font-weight: 650;
      color: var(--el-text-color-primary);
    }

    p {
      margin: 0;
      font-size: 13px;
      color: var(--el-text-color-secondary);
    }
  }

  .actions {
    display: flex;
    flex-wrap: wrap;
    justify-content: flex-end;
    gap: 8px;
  }

  .zone-grid {
    row-gap: 12px;
  }

  .zone-card {
    min-height: 520px;
    border-radius: 8px;
  }

  .zone-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
  }

  .zone-title {
    font-weight: 650;
    margin-right: 8px;
  }

  .zone-sub {
    color: var(--el-text-color-secondary);
    font-size: 12px;
  }

  .stock-list {
    display: flex;
    flex-direction: column;
    gap: 4px;
    max-height: 480px;
    overflow-y: auto;
  }

  .stock-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    border: 1px solid var(--el-border-color-lighter);
    border-radius: 6px;
    padding: 6px 10px;
    background: var(--el-fill-color-blank);
    cursor: pointer;
    transition: background 0.15s;

    &:hover {
      background: var(--el-fill-color-light);
    }

    &.matched {
      border-color: var(--el-color-success);
      background: var(--el-color-success-light-9);
    }
  }

  .row-left {
    display: flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
    flex: 0 0 auto;
  }

  .stock-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    flex-shrink: 0;

    &.dot-green { background: #22c55e; }
    &.dot-blue { background: #3b82f6; }
    &.dot-yellow { background: #eab308; }
    &.dot-orange { background: #f97316; }
    &.dot-gray { background: #d1d5db; }
  }

  .stock-info {
    display: flex;
    flex-direction: column;
    gap: 1px;
    min-width: 0;
  }

  .stock-name {
    font-size: 13px;
    font-weight: 500;
    color: var(--el-text-color-primary);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 90px;
  }

  .stock-code {
    font-size: 11px;
    color: var(--el-text-color-secondary);
  }

  .row-right {
    display: flex;
    align-items: center;
    gap: 6px;
    flex: 1;
    justify-content: flex-end;
  }

  .source-tag {
    flex-shrink: 0;
  }

  .crowding-tag {
    flex-shrink: 0;
  }

  .metrics {
    display: flex;
    align-items: center;
    gap: 8px;
    color: var(--el-text-color-regular);
    font-size: 11px;
    white-space: nowrap;

    span { display: inline-block; }
    span::before { content: ' '; }
    span:first-child::before { content: ''; }
  }
}

@media (max-width: 768px) {
  .stock-pool {
    .toolbar {
      flex-direction: column;
    }

    .actions {
      justify-content: flex-start;
    }
  }
}
</style>
