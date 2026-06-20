<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
const router = useRouter()
const route = useRoute()

// Optional modules: CDC is hidden unless /features reports it enabled (D3 #t53).
const cdcEnabled = ref(false)
onMounted(async () => {
  try {
    const r = await fetch('/api/v1/features')
    if (r.ok) cdcEnabled.value = !!(await r.json())?.cdc?.enabled
  } catch {
    // default: CDC hidden
  }
})
</script>

<template>
  <el-container style="height: 100vh">
    <el-header style="background: #1a1a2e; display: flex; align-items: center; padding: 0 24px;">
      <div style="color: #fff; font-size: 20px; font-weight: bold; cursor: pointer; display: flex; align-items: baseline;" @click="router.push('/')">
        <span style="color: #e23d3d; font-size: 24px; font-weight: 900; margin-right: 4px;">Ti</span><span style="color: #fff;">MS</span>
      </div>
      <el-menu
        :default-active="route.path"
        mode="horizontal"
        :ellipsis="false"
        background-color="#1a1a2e"
        text-color="#ccc"
        active-text-color="#409eff"
        style="margin-left: 40px; border: none;"
        router
      >
        <el-menu-item index="/wizard">
          <el-icon><Connection /></el-icon>
          <span>新建迁移</span>
        </el-menu-item>
        <el-menu-item index="/tasks">
          <el-icon><Monitor /></el-icon>
          <span>任务监控</span>
        </el-menu-item>
        <el-menu-item index="/history">
          <el-icon><Clock /></el-icon>
          <span>迁移历史</span>
        </el-menu-item>
        <el-menu-item index="/assess">
          <el-icon><DataAnalysis /></el-icon>
          <span>兼容性评估</span>
        </el-menu-item>
        <el-menu-item index="/cdc" v-if="cdcEnabled">
          <el-icon><DataLine /></el-icon>
          <span>CDC 增量同步</span>
        </el-menu-item>
      </el-menu>
    </el-header>
    <el-main style="background: #f5f7fa; padding: 24px;">
      <router-view />
    </el-main>
  </el-container>
</template>

<style>
body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
.el-header { padding: 0 24px; }
</style>
