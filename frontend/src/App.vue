<template>
  <div class="app-shell">
    <header class="app-header">
      <div class="app-brand">
        <span class="app-brand-dot"></span>
        <div>
          <p class="app-brand-name">RFlowStudio</p>
          <p class="app-brand-subtitle">统计工作台</p>
        </div>
      </div>

      <nav class="app-nav">
        <RouterLink to="/" class="app-nav-link">数据集</RouterLink>

        <RouterLink
          v-if="currentDatasetId"
          :to="{ name: 'dataset-detail', params: { datasetId: currentDatasetId } }"
          class="app-nav-link"
        >
          预览
        </RouterLink>
        <span v-else class="app-nav-link is-muted">预览</span>

        <RouterLink
          v-if="currentDatasetId"
          :to="{ name: 'dataset-cleaning', params: { datasetId: currentDatasetId } }"
          class="app-nav-link"
        >
          清洗
        </RouterLink>
        <span v-else class="app-nav-link is-muted">清洗</span>

        <span class="app-nav-link is-muted">分析</span>
        <span class="app-nav-link is-muted">报告</span>
      </nav>
    </header>

    <main class="app-main">
      <RouterView />
    </main>
  </div>
</template>

<script setup>
import { computed, ref, watch } from 'vue';
import { useRoute } from 'vue-router';

const CURRENT_DATASET_STORAGE_KEY = 'rflow_current_dataset_id';

const route = useRoute();
const rememberedDatasetId = ref('');

function readRememberedDatasetId() {
  if (typeof window === 'undefined') {
    return '';
  }

  return window.localStorage.getItem(CURRENT_DATASET_STORAGE_KEY) ?? '';
}

function rememberDatasetId(datasetId) {
  rememberedDatasetId.value = datasetId;

  if (typeof window === 'undefined') {
    return;
  }

  window.localStorage.setItem(CURRENT_DATASET_STORAGE_KEY, datasetId);
}

rememberedDatasetId.value = readRememberedDatasetId();

watch(
  () => route.params.datasetId,
  (rawDatasetId) => {
    if (typeof rawDatasetId !== 'string' || !rawDatasetId.trim()) {
      return;
    }

    rememberDatasetId(rawDatasetId.trim());
  },
  { immediate: true },
);

const currentDatasetId = computed(() => {
  const rawDatasetId = route.params.datasetId;
  if (typeof rawDatasetId === 'string' && rawDatasetId.trim()) {
    return rawDatasetId.trim();
  }

  return rememberedDatasetId.value;
});
</script>
