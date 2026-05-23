<template>
  <div class="portfolio-home">
    <div class="page-head">
      <div>
        <h1>组合管理</h1>
        <p>股票池、模拟交易和组合风控的统一入口。</p>
      </div>
    </div>

    <div class="entry-grid">
      <el-card shadow="hover" class="entry-card" @click="$router.push('/portfolio/stock-pool')">
        <div class="entry-title">股票池</div>
        <div class="entry-meta">SCAN / WATCH / CANDIDATE / CONVICTION</div>
      </el-card>
    </div>
  </div>
</template>

<script setup lang="ts">
defineOptions({ name: 'PortfolioHome' })
</script>

<style lang="scss" scoped>
.portfolio-home {
  .page-head {
    margin-bottom: 16px;

    h1 {
      margin: 0 0 6px;
      font-size: 22px;
      font-weight: 650;
      color: var(--el-text-color-primary);
    }

    p {
      margin: 0;
      color: var(--el-text-color-secondary);
      font-size: 13px;
    }
  }

  .entry-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    gap: 16px;
  }

  .entry-card {
    cursor: pointer;
    border-radius: 8px;
  }

  .entry-title {
    font-size: 16px;
    font-weight: 650;
    margin-bottom: 8px;
  }

  .entry-meta {
    color: var(--el-text-color-secondary);
    font-size: 13px;
  }
}
</style>
