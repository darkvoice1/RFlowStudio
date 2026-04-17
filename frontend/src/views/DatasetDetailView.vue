<template>
  <div class="dataset-detail-page">
    <section class="detail-topbar">
      <div class="crumb-row">
        <RouterLink to="/" class="text-link">数据集列表</RouterLink>
        <span class="crumb-separator">/</span>
        <span class="crumb-current">预览</span>
      </div>
      <div class="detail-topbar-actions">
        <RouterLink
          :to="{ name: 'dataset-cleaning', params: { datasetId } }"
          class="secondary-button is-compact"
        >
          进入清洗
        </RouterLink>
        <span v-if="datasetDetail" class="detail-topbar-id">
          {{ datasetDetail.id }}
        </span>
      </div>
    </section>

    <section v-if="pageError" class="panel-card">
      <p class="feedback error-text">{{ pageError }}</p>
    </section>

    <template v-else>
      <section class="panel-card detail-hero">
        <div class="detail-hero-main">
          <div class="detail-name-row">
            <p class="section-label">数据集</p>
            <span
              v-if="datasetDetail"
              class="status-badge"
              :class="`status-${datasetDetail.status}`"
            >
              {{ statusLabelMap[datasetDetail.status] ?? datasetDetail.status }}
            </span>
          </div>
          <p class="detail-dataset-name">
            {{ datasetDetail?.name ?? '正在读取数据集信息...' }}
          </p>
          <p class="detail-file-name">
            {{ datasetDetail?.file_name ?? '请稍候' }}
          </p>
        </div>

        <div class="detail-metric-row">
          <div class="metric-chip">
            <span>大小</span>
            <strong>{{ formatFileSize(datasetDetail?.size_bytes) }}</strong>
          </div>
          <div class="metric-chip">
            <span>行数</span>
            <strong>{{ profileSummary.rowCountLabel }}</strong>
          </div>
          <div class="metric-chip">
            <span>字段</span>
            <strong>{{ profileSummary.columnCountLabel }}</strong>
          </div>
          <div class="metric-chip">
            <span>创建时间</span>
            <strong>{{ formatDateTime(datasetDetail?.created_at) }}</strong>
          </div>
        </div>
      </section>

      <section class="detail-layout">
        <aside class="detail-sidebar">
          <article class="panel-card info-card">
            <div class="section-head">
              <div>
                <p class="section-label">元信息</p>
                <p class="section-title">数据集概况</p>
              </div>
              <button
                type="button"
                class="text-button"
                :disabled="detailLoading"
                @click="loadDatasetDetail"
              >
                {{ detailLoading ? '读取中...' : '刷新' }}
              </button>
            </div>

            <dl class="detail-info-list">
              <div>
                <dt>扩展名</dt>
                <dd>{{ datasetDetail?.extension ?? '-' }}</dd>
              </div>
              <div>
                <dt>状态</dt>
                <dd>{{ datasetDetail ? statusLabelMap[datasetDetail.status] ?? datasetDetail.status : '-' }}</dd>
              </div>
              <div>
                <dt>存储路径</dt>
                <dd class="detail-path">{{ datasetDetail?.stored_path ?? '-' }}</dd>
              </div>
            </dl>
          </article>

          <article class="panel-card info-card">
            <div class="section-head">
              <div>
                <p class="section-label">字段画像</p>
                <p class="section-title">数据结构</p>
              </div>
              <button
                type="button"
                class="text-button"
                :disabled="profileLoading"
                @click="loadDatasetProfile"
              >
                {{ profileLoading ? '读取中...' : '刷新' }}
              </button>
            </div>

            <p v-if="profileError" class="feedback error-text">{{ profileError }}</p>
            <template v-else>
              <dl class="detail-info-list compact">
                <div>
                  <dt>总行数</dt>
                  <dd>{{ profileSummary.rowCountLabel }}</dd>
                </div>
                <div>
                  <dt>字段数</dt>
                  <dd>{{ profileSummary.columnCountLabel }}</dd>
                </div>
                <div>
                  <dt>格式</dt>
                  <dd>{{ datasetProfile?.profile_format ?? '-' }}</dd>
                </div>
              </dl>

              <div v-if="topColumns.length" class="column-pills">
                <span
                  v-for="column in topColumns"
                  :key="column.name"
                  class="column-pill"
                >
                  {{ column.name }}
                </span>
              </div>
              <p v-else class="muted-text">字段信息会显示在这里。</p>
            </template>
          </article>
        </aside>

        <div class="detail-main">
          <article class="panel-card">
            <div class="section-head">
              <div>
                <p class="section-label">预览</p>
                <p class="section-title">原始数据片段</p>
              </div>
              <div class="section-actions">
                <button
                  type="button"
                  class="text-button"
                  :disabled="previewLoading || !canGoPrevious"
                  @click="loadPreviewPage(previewOffset - previewLimit)"
                >
                  上一页
                </button>
                <button
                  type="button"
                  class="text-button"
                  :disabled="previewLoading || !canGoNext"
                  @click="loadPreviewPage(previewOffset + previewLimit)"
                >
                  下一页
                </button>
              </div>
            </div>

            <p v-if="previewError" class="feedback error-text">{{ previewError }}</p>
            <template v-else>
              <p class="section-copy">
                当前显示第 {{ previewRangeText }} 行，单页 {{ previewLimit }} 条。
              </p>

              <div v-if="previewLoading && !datasetPreview" class="empty-state">
                正在读取预览数据...
              </div>
              <div v-else-if="!datasetPreview?.rows?.length" class="empty-state">
                当前还没有可展示的预览数据。
              </div>
              <div v-else class="table-shell">
                <table class="data-table">
                  <thead>
                    <tr>
                      <th v-for="column in datasetPreview.columns" :key="column">
                        {{ column }}
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="(row, rowIndex) in datasetPreview.rows" :key="`${previewOffset}-${rowIndex}`">
                      <td v-for="column in datasetPreview.columns" :key="column">
                        {{ normalizeCellValue(row[column]) }}
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </template>
          </article>

          <article class="panel-card">
            <div class="section-head">
              <div>
                <p class="section-label">字段</p>
                <p class="section-title">字段分析</p>
              </div>
              <RouterLink
                :to="{ name: 'dataset-cleaning', params: { datasetId } }"
                class="text-link"
              >
                去清洗页
              </RouterLink>
            </div>

            <p class="section-copy">
              这里先承接字段类型、缺失情况和样本值，后面清洗页与分析页都会继续复用这批信息。
            </p>

            <div v-if="profileLoading && !datasetProfile" class="empty-state">
              正在读取字段信息...
            </div>
            <div v-else-if="!datasetProfile?.columns?.length" class="empty-state">
              当前没有字段画像信息。
            </div>
            <div v-else class="table-shell">
              <table class="data-table">
                <thead>
                  <tr>
                    <th>字段名</th>
                    <th>类型</th>
                    <th>可空</th>
                    <th>缺失值</th>
                    <th>唯一值</th>
                    <th>样本值</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="column in datasetProfile.columns" :key="column.name">
                    <td>{{ column.name }}</td>
                    <td>{{ typeLabelMap[column.inferred_type] ?? column.inferred_type }}</td>
                    <td>{{ column.nullable ? '是' : '否' }}</td>
                    <td>{{ column.missing_count }}</td>
                    <td>{{ column.unique_count }}</td>
                    <td>{{ formatSampleValues(column.sample_values) }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </article>
        </div>
      </section>
    </template>
  </div>
</template>

<script setup>
import { computed, ref, watch } from 'vue';
import { useRoute } from 'vue-router';

import { apiRequest, formatDateTime, formatFileSize } from '../lib/api';

const route = useRoute();

const datasetDetail = ref(null);
const datasetPreview = ref(null);
const datasetProfile = ref(null);

const detailLoading = ref(false);
const previewLoading = ref(false);
const profileLoading = ref(false);

const pageError = ref('');
const previewError = ref('');
const profileError = ref('');

const previewLimit = ref(20);
const previewOffset = ref(0);

const statusLabelMap = {
  draft: '草稿',
  processing: '处理中',
  ready: '可用',
  failed: '失败',
};

const typeLabelMap = {
  integer: '整数',
  float: '浮点数',
  boolean: '布尔',
  string: '字符串',
  empty: '空列',
};

const datasetId = computed(() => String(route.params.datasetId ?? ''));

const canGoPrevious = computed(() => previewOffset.value > 0);
const canGoNext = computed(() => Boolean(datasetPreview.value?.has_more));

const previewRangeText = computed(() => {
  if (!datasetPreview.value?.rows?.length) {
    return '0 - 0';
  }

  const start = previewOffset.value + 1;
  const end = previewOffset.value + datasetPreview.value.rows.length;
  return `${start} - ${end}`;
});

const profileSummary = computed(() => ({
  rowCountLabel:
    typeof datasetProfile.value?.row_count === 'number'
      ? String(datasetProfile.value.row_count)
      : '-',
  columnCountLabel:
    typeof datasetProfile.value?.column_count === 'number'
      ? String(datasetProfile.value.column_count)
      : '-',
}));

const topColumns = computed(() =>
  Array.isArray(datasetProfile.value?.columns)
    ? datasetProfile.value.columns.slice(0, 6)
    : [],
);

async function loadDatasetDetail() {
  detailLoading.value = true;

  try {
    datasetDetail.value = await apiRequest(`/datasets/${datasetId.value}`);
  } catch (error) {
    pageError.value = error instanceof Error ? error.message : '数据集详情读取失败。';
  } finally {
    detailLoading.value = false;
  }
}

async function loadDatasetProfile() {
  profileLoading.value = true;
  profileError.value = '';

  try {
    datasetProfile.value = await apiRequest(`/datasets/${datasetId.value}/profile`);
  } catch (error) {
    profileError.value = error instanceof Error ? error.message : '字段画像读取失败。';
  } finally {
    profileLoading.value = false;
  }
}

async function loadPreviewPage(offset = 0) {
  previewLoading.value = true;
  previewError.value = '';

  try {
    const payload = await apiRequest(
      `/datasets/${datasetId.value}/preview?offset=${Math.max(offset, 0)}&limit=${previewLimit.value}`,
    );

    datasetPreview.value = payload;
    previewOffset.value = payload.offset ?? 0;
    previewLimit.value = payload.limit ?? previewLimit.value;
  } catch (error) {
    previewError.value = error instanceof Error ? error.message : '预览数据读取失败。';
  } finally {
    previewLoading.value = false;
  }
}

async function loadDatasetPage() {
  pageError.value = '';
  datasetDetail.value = null;
  datasetPreview.value = null;
  datasetProfile.value = null;
  previewOffset.value = 0;

  await loadDatasetDetail();
  if (pageError.value) {
    return;
  }

  await Promise.all([loadPreviewPage(0), loadDatasetProfile()]);
}

function normalizeCellValue(value) {
  if (value === null || value === undefined || value === '') {
    return '—';
  }

  return String(value);
}

function formatSampleValues(values) {
  if (!Array.isArray(values) || !values.length) {
    return '—';
  }

  return values.join('，');
}

watch(
  datasetId,
  async () => {
    if (!datasetId.value) {
      pageError.value = '缺少数据集 ID。';
      return;
    }

    await loadDatasetPage();
  },
  { immediate: true },
);
</script>
