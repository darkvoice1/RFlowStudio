<template>
  <div class="dataset-report-page">
    <section class="detail-topbar">
      <div class="crumb-row">
        <RouterLink to="/" class="text-link">数据集列表</RouterLink>
        <span class="crumb-separator">/</span>
        <RouterLink :to="{ name: 'dataset-detail', params: { datasetId } }" class="text-link">
          预览
        </RouterLink>
        <span class="crumb-separator">/</span>
        <RouterLink :to="{ name: 'dataset-cleaning', params: { datasetId } }" class="text-link">
          清洗
        </RouterLink>
        <span class="crumb-separator">/</span>
        <RouterLink :to="{ name: 'dataset-analysis', params: { datasetId } }" class="text-link">
          分析
        </RouterLink>
        <span class="crumb-separator">/</span>
        <span class="crumb-current">报告</span>
      </div>
      <span v-if="datasetDetail" class="detail-topbar-id">{{ datasetDetail.id }}</span>
    </section>

    <section v-if="pageError" class="panel-card">
      <p class="feedback error-text">{{ pageError }}</p>
    </section>

    <template v-else>
      <section class="panel-card detail-hero">
        <div class="detail-hero-main">
          <div class="detail-name-row">
            <p class="section-label">分析报告</p>
            <span v-if="selectedRecord" class="status-badge status-ready">
              {{ analysisTypeLabelMap[selectedRecord.analysis_type] ?? selectedRecord.analysis_type }}
            </span>
          </div>
          <p class="detail-dataset-name">
            {{ datasetDetail?.name ?? '正在读取数据集信息...' }}
          </p>
          <p class="detail-file-name">
            这里统一承接历史分析记录的报告草稿、HTML 报告和复现脚本查看。
          </p>
        </div>

        <div class="detail-metric-row">
          <div class="metric-chip">
            <span>历史分析</span>
            <strong>{{ analysisHistory.length }}</strong>
          </div>
          <div class="metric-chip">
            <span>当前模板</span>
            <strong>{{ currentTemplateName }}</strong>
          </div>
          <div class="metric-chip">
            <span>导出格式</span>
            <strong>{{ supportedExportLabel }}</strong>
          </div>
          <div class="metric-chip">
            <span>当前面板</span>
            <strong>{{ activePanelLabel }}</strong>
          </div>
        </div>
      </section>

      <section class="report-layout">
        <aside class="report-sidebar">
          <article class="panel-card info-card">
            <div class="section-head">
              <div>
                <p class="section-label">记录</p>
                <p class="section-title">分析历史</p>
              </div>
              <button
                type="button"
                class="text-button"
                :disabled="historyLoading"
                @click="loadAnalysisHistory"
              >
                {{ historyLoading ? '读取中...' : '刷新' }}
              </button>
            </div>

            <p v-if="historyError" class="feedback error-text">{{ historyError }}</p>
            <div v-if="historyLoading && !analysisHistory.length" class="empty-state compact-empty-state">
              <p>正在读取分析历史...</p>
            </div>
            <div v-else-if="!analysisHistory.length" class="empty-state compact-empty-state">
              <p>当前还没有可查看的分析记录。</p>
            </div>
            <div v-else class="history-list">
              <article
                v-for="record in analysisHistory"
                :key="record.id"
                class="history-card"
                :class="{ 'is-active': selectedRecordId === record.id }"
              >
                <button
                  type="button"
                  class="history-select-button"
                  @click="selectRecord(record.id)"
                >
                  <span class="step-order">{{ formatDateTime(record.created_at) }}</span>
                  <strong class="history-select-title">
                    {{ analysisTypeLabelMap[record.analysis_type] ?? record.analysis_type }}
                  </strong>
                  <span class="muted-text">{{ formatVariableList(record.variables) }}</span>
                  <span v-if="record.group_variable" class="muted-text">
                    分组：{{ record.group_variable }}
                  </span>
                </button>
              </article>
            </div>
          </article>

          <article class="panel-card info-card">
            <div class="section-head">
              <div>
                <p class="section-label">模板</p>
                <p class="section-title">报告样式</p>
              </div>
            </div>

            <div v-if="templateOptions.length" class="template-list">
              <button
                v-for="template in templateOptions"
                :key="template.key"
                type="button"
                class="template-card"
                :class="{ 'is-active': selectedTemplate === template.key }"
                @click="changeTemplate(template.key)"
              >
                <strong>{{ template.name }}</strong>
                <span>{{ template.description }}</span>
              </button>
            </div>
            <div v-else class="empty-state compact-empty-state">
              <p>选中分析记录后会在这里显示可用模板。</p>
            </div>
          </article>
        </aside>

        <div class="report-main">
          <article class="panel-card">
            <div class="section-head">
              <div>
                <p class="section-label">查看</p>
                <p class="section-title">报告与脚本</p>
              </div>
              <div class="section-actions">
                <button
                  type="button"
                  class="secondary-button is-compact"
                  :class="{ 'is-selected': activePanel === 'report' }"
                  :disabled="!selectedRecordId"
                  @click="changePanel('report')"
                >
                  报告
                </button>
                <button
                  type="button"
                  class="secondary-button is-compact"
                  :class="{ 'is-selected': activePanel === 'script' }"
                  :disabled="!selectedRecordId"
                  @click="changePanel('script')"
                >
                  脚本
                </button>
              </div>
            </div>

            <div v-if="selectedRecord" class="report-toolbar">
              <div class="report-toolbar-copy">
                <p class="section-copy">
                  {{ selectedRecord.result.summary.title }}
                </p>
                <p class="muted-text">
                  文件：{{ selectedRecord.result.file_name }} · 记录时间：{{ formatDateTime(selectedRecord.created_at) }}
                </p>
              </div>
              <div class="section-actions">
                <button
                  type="button"
                  class="text-button"
                  :disabled="reportLoading || scriptLoading"
                  @click="refreshSelectedRecord"
                >
                  {{ reportLoading || scriptLoading ? '刷新中...' : '刷新内容' }}
                </button>
                <button
                  type="button"
                  class="text-button"
                  :disabled="!selectedRecordId"
                  @click="openHtmlReport"
                >
                  打开 HTML
                </button>
                <button
                  type="button"
                  class="text-button"
                  :disabled="!scriptDraft?.script"
                  @click="copyScript"
                >
                  复制脚本
                </button>
              </div>
            </div>

            <p v-if="actionMessage" class="feedback success-text">{{ actionMessage }}</p>
            <p v-if="reportError" class="feedback error-text">{{ reportError }}</p>
            <p v-if="scriptError" class="feedback error-text">{{ scriptError }}</p>

            <div v-if="!selectedRecordId" class="empty-state">
              <p>先从左侧选择一条分析记录，再查看报告或脚本。</p>
            </div>

            <template v-else-if="activePanel === 'report'">
              <div v-if="reportLoading && !reportDraft" class="empty-state">
                <p>正在生成报告草稿...</p>
              </div>
              <div v-else-if="!reportDraft" class="empty-state">
                <p>当前报告草稿还不可用。</p>
              </div>
              <div v-else class="report-stack">
                <div class="panel-card inset-panel">
                  <p class="section-label">标题</p>
                  <p class="section-title">{{ reportDraft.title }}</p>
                  <div class="report-meta-grid">
                    <div class="metric-chip">
                      <span>模板</span>
                      <strong>{{ currentTemplateName }}</strong>
                    </div>
                    <div class="metric-chip">
                      <span>分析类型</span>
                      <strong>{{ analysisTypeLabelMap[reportDraft.analysis_type] ?? reportDraft.analysis_type }}</strong>
                    </div>
                    <div class="metric-chip">
                      <span>数据文件</span>
                      <strong>{{ reportDraft.file_name }}</strong>
                    </div>
                    <div class="metric-chip">
                      <span>生成时间</span>
                      <strong>{{ formatDateTime(reportDraft.generated_at) }}</strong>
                    </div>
                  </div>
                </div>

                <article
                  v-for="section in reportDraft.sections"
                  :key="section.key"
                  class="panel-card inset-panel report-section-card"
                >
                  <p class="section-label">报告区块</p>
                  <p class="section-title">{{ section.title }}</p>

                  <dl v-if="section.section_type === 'summary'" class="report-summary-list">
                    <div
                      v-for="(value, key) in section.content"
                      :key="`${section.key}-${key}`"
                    >
                      <dt>{{ formatSectionKey(key) }}</dt>
                      <dd>{{ formatSectionValue(value) }}</dd>
                    </div>
                  </dl>

                  <ul v-else-if="section.section_type === 'text'" class="plain-list">
                    <li
                      v-for="(item, index) in section.content.items ?? []"
                      :key="`${section.key}-${index}`"
                    >
                      {{ item }}
                    </li>
                  </ul>

                  <div v-else-if="section.section_type === 'table'" class="report-table-stack">
                    <div
                      v-for="table in section.content.items ?? []"
                      :key="table.key"
                      class="report-table-card"
                    >
                      <p class="section-copy">{{ table.title }}</p>
                      <div v-if="table.rows?.length" class="table-shell">
                        <table class="data-table">
                          <thead>
                            <tr>
                              <th v-for="column in table.columns" :key="column">{{ column }}</th>
                            </tr>
                          </thead>
                          <tbody>
                            <tr v-for="(row, index) in table.rows" :key="`${table.key}-${index}`">
                              <td v-for="column in table.columns" :key="column">
                                {{ normalizeResultCell(row[column]) }}
                              </td>
                            </tr>
                          </tbody>
                        </table>
                      </div>
                      <div v-else class="empty-state compact-empty-state">
                        <p>当前结果表没有可展示数据。</p>
                      </div>
                    </div>
                  </div>

                  <ul v-else-if="section.section_type === 'plot_list'" class="plain-list">
                    <li
                      v-for="plot in section.content.items ?? []"
                      :key="plot.key"
                    >
                      {{ plot.title }} · {{ plotTypeLabelMap[plot.plot_type] ?? plot.plot_type }}
                    </li>
                  </ul>

                  <pre v-else class="script-block"><code>{{ section.content.script ?? '当前没有可展示的复现脚本。' }}</code></pre>
                </article>
              </div>
            </template>

            <template v-else>
              <div v-if="scriptLoading && !scriptDraft" class="empty-state">
                <p>正在读取分析脚本...</p>
              </div>
              <div v-else-if="!scriptDraft?.script" class="empty-state">
                <p>当前还没有可展示的分析脚本。</p>
              </div>
              <div v-else class="script-panel">
                <div class="script-meta">
                  <span>分析类型：{{ analysisTypeLabelMap[scriptDraft.analysis_type] ?? scriptDraft.analysis_type }}</span>
                  <span>文件：{{ scriptDraft.file_name }}</span>
                  <span>记录 ID：{{ scriptDraft.analysis_record_id }}</span>
                </div>
                <pre class="script-block"><code>{{ scriptDraft.script }}</code></pre>
              </div>
            </template>
          </article>
        </div>
      </section>
    </template>
  </div>
</template>

<script setup>
import { computed, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { apiRequest, buildApiUrl, formatDateTime } from '../lib/api';

const route = useRoute();
const router = useRouter();

const datasetId = computed(() => String(route.params.datasetId ?? ''));

const datasetDetail = ref(null);
const analysisHistory = ref([]);
const selectedRecordId = ref('');
const selectedTemplate = ref('general');
const activePanel = ref('report');
const reportDraft = ref(null);
const scriptDraft = ref(null);

const pageError = ref('');
const historyError = ref('');
const reportError = ref('');
const scriptError = ref('');
const actionMessage = ref('');

const detailLoading = ref(false);
const historyLoading = ref(false);
const reportLoading = ref(false);
const scriptLoading = ref(false);

const allowedTemplateKeys = new Set([
  'general',
  'questionnaire_analysis',
  'pre_post_experiment',
  'group_comparison',
]);

const analysisTypeLabelMap = {
  descriptive_statistics: '描述统计',
  independent_samples_t_test: '独立样本 t 检验',
  one_way_anova: '单因素方差分析',
  chi_square_test: '卡方检验',
  correlation_analysis: '相关分析',
};

const sectionKeyLabelMap = {
  dataset_name: '数据集名称',
  file_name: '数据文件',
  analysis_type: '分析方法',
  variables: '分析变量',
  group_variable: '分组变量',
  missing_value_strategy: '缺失值处理方式',
  title: '结果标题',
  effective_row_count: '有效样本量',
  excluded_row_count: '剔除样本量',
  note: '结果说明',
};

const plotTypeLabelMap = {
  histogram: '直方图',
  boxplot: '箱线图',
  bar_chart: '条形图',
  scatter_plot: '散点图',
  heatmap: '热力图',
  grouped_bar_chart: '分组条形图',
  grouped_boxplot: '分组箱线图',
};

const selectedRecord = computed(() =>
  analysisHistory.value.find((item) => item.id === selectedRecordId.value) ?? null,
);

const templateOptions = computed(() => reportDraft.value?.available_templates ?? []);

const currentTemplateName = computed(() => {
  const matched = templateOptions.value.find((item) => item.key === selectedTemplate.value);
  return matched?.name ?? '通用分析模板';
});

const supportedExportLabel = computed(() => {
  const formats = reportDraft.value?.supported_export_formats ?? [];
  return formats.length ? formats.join(' / ').toUpperCase() : 'HTML';
});

const activePanelLabel = computed(() => (activePanel.value === 'script' ? '脚本' : '报告'));

function normalizeQueryValue(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function normalizeTemplateKey(value) {
  return allowedTemplateKeys.has(value) ? value : 'general';
}

function syncStateFromQuery() {
  const queryRecordId = normalizeQueryValue(route.query.recordId);
  const queryTemplate = normalizeQueryValue(route.query.template);
  const queryPanel = normalizeQueryValue(route.query.panel);

  if (queryRecordId) {
    selectedRecordId.value = queryRecordId;
  }

  if (queryTemplate) {
    selectedTemplate.value = normalizeTemplateKey(queryTemplate);
  }

  activePanel.value = queryPanel === 'script' ? 'script' : 'report';
}

function syncQuery() {
  const nextQuery = {
    ...route.query,
    recordId: selectedRecordId.value || undefined,
    template: selectedTemplate.value || undefined,
    panel: activePanel.value || undefined,
  };

  const currentRecordId = normalizeQueryValue(route.query.recordId);
  const currentTemplate = normalizeTemplateKey(normalizeQueryValue(route.query.template) || 'general');
  const currentPanel = normalizeQueryValue(route.query.panel) === 'script' ? 'script' : 'report';

  if (
    currentRecordId === (selectedRecordId.value || '')
    && currentTemplate === selectedTemplate.value
    && currentPanel === activePanel.value
  ) {
    return;
  }

  router.replace({
    name: 'dataset-report',
    params: { datasetId: datasetId.value },
    query: nextQuery,
  });
}

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

async function loadAnalysisHistory() {
  historyLoading.value = true;
  historyError.value = '';

  try {
    const payload = await apiRequest(`/datasets/${datasetId.value}/analysis-records`);
    analysisHistory.value = Array.isArray(payload.items) ? payload.items : [];

    const recordExists = analysisHistory.value.some((item) => item.id === selectedRecordId.value);
    if (!recordExists) {
      selectedRecordId.value = analysisHistory.value[0]?.id ?? '';
    }
  } catch (error) {
    historyError.value = error instanceof Error ? error.message : '分析历史读取失败。';
  } finally {
    historyLoading.value = false;
  }
}

async function loadReportDraft() {
  if (!selectedRecordId.value) {
    reportDraft.value = null;
    return;
  }

  reportLoading.value = true;
  reportError.value = '';

  try {
    reportDraft.value = await apiRequest(
      `/datasets/${datasetId.value}/analysis-records/${selectedRecordId.value}/report-draft?template_key=${selectedTemplate.value}`,
    );
  } catch (error) {
    reportDraft.value = null;
    reportError.value = error instanceof Error ? error.message : '报告草稿读取失败。';
  } finally {
    reportLoading.value = false;
  }
}

async function loadScriptDraft() {
  if (!selectedRecordId.value) {
    scriptDraft.value = null;
    return;
  }

  scriptLoading.value = true;
  scriptError.value = '';

  try {
    scriptDraft.value = await apiRequest(
      `/datasets/${datasetId.value}/analysis-records/${selectedRecordId.value}/script`,
    );
  } catch (error) {
    scriptDraft.value = null;
    scriptError.value = error instanceof Error ? error.message : '分析脚本读取失败。';
  } finally {
    scriptLoading.value = false;
  }
}

async function loadPage() {
  pageError.value = '';
  actionMessage.value = '';
  reportDraft.value = null;
  scriptDraft.value = null;
  analysisHistory.value = [];
  selectedRecordId.value = normalizeQueryValue(route.query.recordId);
  selectedTemplate.value = normalizeTemplateKey(normalizeQueryValue(route.query.template) || 'general');
  activePanel.value = normalizeQueryValue(route.query.panel) === 'script' ? 'script' : 'report';

  await loadDatasetDetail();
  if (pageError.value) {
    return;
  }

  await loadAnalysisHistory();
}

function selectRecord(recordId) {
  selectedRecordId.value = recordId;
  actionMessage.value = '已切换到对应分析记录。';
}

function changeTemplate(templateKey) {
  selectedTemplate.value = normalizeTemplateKey(templateKey);
  actionMessage.value = '报告模板已切换。';
}

function changePanel(panel) {
  activePanel.value = panel;
}

async function refreshSelectedRecord() {
  actionMessage.value = '';
  await Promise.all([loadReportDraft(), loadScriptDraft()]);
  if (!reportError.value && !scriptError.value) {
    actionMessage.value = '当前报告与脚本已刷新。';
  }
}

function openHtmlReport() {
  if (!selectedRecordId.value) {
    return;
  }

  const reportUrl = buildApiUrl(
    `/datasets/${datasetId.value}/analysis-records/${selectedRecordId.value}/report-html?template_key=${selectedTemplate.value}`,
  );
  window.open(reportUrl, '_blank', 'noopener');
}

async function copyScript() {
  if (!scriptDraft.value?.script || !navigator.clipboard) {
    return;
  }

  try {
    await navigator.clipboard.writeText(scriptDraft.value.script);
    actionMessage.value = '分析脚本已复制到剪贴板。';
  } catch (error) {
    scriptError.value = error instanceof Error ? error.message : '复制脚本失败。';
  }
}

function formatVariableList(variables) {
  if (!Array.isArray(variables) || !variables.length) {
    return '—';
  }

  return variables.join('，');
}

function formatSectionKey(key) {
  return sectionKeyLabelMap[key] ?? key;
}

function formatSectionValue(value) {
  if (value === null || value === undefined || value === '') {
    return '无';
  }

  if (Array.isArray(value)) {
    return value.length ? value.join('，') : '无';
  }

  if (typeof value === 'string' && analysisTypeLabelMap[value]) {
    return analysisTypeLabelMap[value];
  }

  return String(value);
}

function normalizeResultCell(value) {
  if (value === null || value === undefined || value === '') {
    return '—';
  }

  if (typeof value === 'object') {
    return JSON.stringify(value);
  }

  return String(value);
}

watch(
  () => route.query,
  () => {
    syncStateFromQuery();
  },
);

watch(
  datasetId,
  async () => {
    if (!datasetId.value) {
      pageError.value = '缺少数据集 ID。';
      return;
    }

    await loadPage();
  },
  { immediate: true },
);

watch(
  [selectedRecordId, selectedTemplate, activePanel],
  () => {
    syncQuery();
  },
);

watch(
  selectedRecordId,
  async (value) => {
    if (!value) {
      reportDraft.value = null;
      scriptDraft.value = null;
      return;
    }

    await Promise.all([loadReportDraft(), loadScriptDraft()]);
  },
);

watch(
  selectedTemplate,
  async () => {
    if (!selectedRecordId.value) {
      return;
    }

    await loadReportDraft();
  },
);
</script>
