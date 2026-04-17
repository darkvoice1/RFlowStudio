<template>
  <div class="dataset-analysis-page">
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
        <span class="crumb-current">分析</span>
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
            <p class="section-label">统计分析</p>
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
            这里负责把清洗后的数据交给后端分析任务，并把结果、解释、表格和历史记录展示出来。
          </p>
        </div>

        <div class="detail-metric-row">
          <div class="metric-chip">
            <span>字段数</span>
            <strong>{{ columnNames.length }}</strong>
          </div>
          <div class="metric-chip">
            <span>历史分析</span>
            <strong>{{ analysisHistory.length }}</strong>
          </div>
          <div class="metric-chip">
            <span>当前任务</span>
            <strong>{{ currentTask ? taskStatusLabelMap[currentTask.status] ?? currentTask.status : '无' }}</strong>
          </div>
          <div class="metric-chip">
            <span>当前类型</span>
            <strong>{{ analysisTypeLabelMap[analysisType] }}</strong>
          </div>
        </div>
      </section>

      <section class="analysis-layout">
        <aside class="analysis-sidebar">
          <article class="panel-card form-card">
            <div class="section-head">
              <div>
                <p class="section-label">新建分析</p>
                <p class="section-title">配置分析参数</p>
              </div>
            </div>

            <div class="form-grid">
              <label class="field-block">
                <span class="field-label">分析类型</span>
                <select v-model="analysisType" class="form-control">
                  <option value="descriptive_statistics">描述统计</option>
                  <option value="correlation_analysis">相关性分析</option>
                  <option value="independent_samples_t_test">独立样本 t 检验</option>
                  <option value="one_way_anova">单因素方差分析</option>
                  <option value="chi_square_test">卡方检验</option>
                </select>
              </label>

              <div class="field-block">
                <span class="field-label">选择变量</span>
                <div v-if="columnNames.length" class="selection-grid">
                  <button
                    v-for="column in columnNames"
                    :key="column"
                    type="button"
                    class="selection-pill"
                    :class="{ 'is-active': selectedVariables.includes(column) }"
                    @click="toggleVariable(column)"
                  >
                    {{ column }}
                  </button>
                </div>
                <div v-else class="empty-state compact-empty-state">
                  <p>当前没有可选字段。</p>
                </div>
              </div>

              <label v-if="requiresGroupVariable" class="field-block">
                <span class="field-label">分组变量</span>
                <select v-model="groupVariable" class="form-control">
                  <option value="">请选择字段</option>
                  <option v-for="column in columnNames" :key="column" :value="column">
                    {{ column }}
                  </option>
                </select>
              </label>
            </div>

            <p class="section-copy">{{ analysisHelpText }}</p>

            <div class="button-row">
              <button
                type="button"
                class="primary-button"
                :disabled="submittingAnalysis || !canSubmitAnalysis"
                @click="submitAnalysis"
              >
                {{ submittingAnalysis ? '提交中...' : '发起分析任务' }}
              </button>
              <button
                type="button"
                class="secondary-button"
                :disabled="submittingAnalysis"
                @click="resetAnalysisForm"
              >
                重置
              </button>
            </div>

            <p v-if="analysisSuccessMessage" class="feedback success-text">
              {{ analysisSuccessMessage }}
            </p>
            <p v-if="analysisErrorMessage" class="feedback error-text">
              {{ analysisErrorMessage }}
            </p>
          </article>

          <article class="panel-card info-card">
            <div class="section-head">
              <div>
                <p class="section-label">任务状态</p>
                <p class="section-title">当前分析任务</p>
              </div>
            </div>

            <div v-if="currentTask" class="task-card">
              <div class="task-row">
                <span>任务类型</span>
                <strong>{{ currentTask.task_type }}</strong>
              </div>
              <div class="task-row">
                <span>当前状态</span>
                <strong>{{ taskStatusLabelMap[currentTask.status] ?? currentTask.status }}</strong>
              </div>
              <div class="task-row">
                <span>创建时间</span>
                <strong>{{ formatDateTime(currentTask.created_at) }}</strong>
              </div>
              <div class="task-row" v-if="currentTask.finished_at">
                <span>结束时间</span>
                <strong>{{ formatDateTime(currentTask.finished_at) }}</strong>
              </div>
              <p v-if="currentTask.error_message" class="feedback error-text">
                {{ currentTask.error_message }}
              </p>
            </div>
            <div v-else class="empty-state compact-empty-state">
              <p>当前没有运行中的分析任务。</p>
            </div>
          </article>
        </aside>

        <div class="analysis-main">
          <article class="panel-card">
            <div class="section-head">
              <div>
                <p class="section-label">分析结果</p>
                <p class="section-title">当前展示结果</p>
              </div>
            </div>

            <div v-if="displayResult" class="result-stack">
              <div class="result-summary-grid">
                <div class="metric-chip">
                  <span>分析类型</span>
                  <strong>{{ analysisTypeLabelMap[displayResult.analysis_type] ?? displayResult.analysis_type }}</strong>
                </div>
                <div class="metric-chip">
                  <span>有效样本</span>
                  <strong>{{ displayResult.summary.effective_row_count ?? '-' }}</strong>
                </div>
                <div class="metric-chip">
                  <span>排除样本</span>
                  <strong>{{ displayResult.summary.excluded_row_count ?? '-' }}</strong>
                </div>
                <div class="metric-chip">
                  <span>缺失值处理</span>
                  <strong>{{ displayResult.summary.missing_value_strategy }}</strong>
                </div>
              </div>

              <div class="panel-card inset-panel">
                <p class="section-label">摘要</p>
                <p class="section-title">{{ displayResult.summary.title }}</p>
                <p class="section-copy" v-if="displayResult.summary.note">
                  {{ displayResult.summary.note }}
                </p>
                <p class="section-copy">
                  变量：{{ formatVariableList(displayResult.variables) }}
                </p>
                <p class="section-copy" v-if="displayResult.group_variable">
                  分组变量：{{ displayResult.group_variable }}
                </p>
                <div v-if="displayRecordId" class="section-actions report-entry-actions">
                  <button
                    type="button"
                    class="secondary-button is-compact"
                    @click="openReportPage(displayRecordId, 'report')"
                  >
                    查看报告
                  </button>
                  <button
                    type="button"
                    class="secondary-button is-compact"
                    @click="openReportPage(displayRecordId, 'script')"
                  >
                    查看脚本
                  </button>
                  <button
                    type="button"
                    class="text-button"
                    @click="openHtmlReport(displayRecordId)"
                  >
                    打开 HTML
                  </button>
                </div>
              </div>

              <div v-if="displayResult.interpretations?.length" class="panel-card inset-panel">
                <p class="section-label">结果解释</p>
                <ul class="plain-list">
                  <li v-for="item in displayResult.interpretations" :key="item">{{ item }}</li>
                </ul>
              </div>

              <div
                v-for="table in displayResult.tables"
                :key="table.key"
                class="panel-card inset-panel"
              >
                <p class="section-label">结果表</p>
                <p class="section-title">{{ table.title }}</p>
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

              <div v-if="displayResult.script_draft" class="panel-card inset-panel">
                <div class="section-head">
                  <div>
                    <p class="section-label">分析脚本</p>
                    <p class="section-title">当前任务返回的脚本草稿</p>
                  </div>
                  <button type="button" class="text-button" @click="copyText(displayResult.script_draft, '分析脚本已复制。')">
                    复制
                  </button>
                </div>
                <pre class="script-block"><code>{{ displayResult.script_draft }}</code></pre>
              </div>
            </div>
            <div v-else class="empty-state">
              <p>当前还没有分析结果。先在左侧选择参数并发起一次分析任务。</p>
            </div>
          </article>

          <article class="panel-card">
            <div class="section-head">
              <div>
                <p class="section-label">历史记录</p>
                <p class="section-title">已保存分析</p>
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
            <div v-if="historyLoading && !analysisHistory.length" class="empty-state">
              <p>正在读取分析历史...</p>
            </div>
            <div v-else-if="!analysisHistory.length" class="empty-state">
              <p>当前还没有分析历史。</p>
            </div>
            <div v-else class="history-list">
              <article
                v-for="record in analysisHistory"
                :key="record.id"
                class="history-card"
                :class="{ 'is-active': displayRecordId === record.id }"
              >
                <div class="history-card-head">
                  <div>
                    <p class="step-order">{{ formatDateTime(record.created_at) }}</p>
                    <h3 class="step-name">
                      {{ analysisTypeLabelMap[record.analysis_type] ?? record.analysis_type }}
                    </h3>
                  </div>
                  <div class="step-badges">
                    <span class="parameter-pill">变量：{{ formatVariableList(record.variables) }}</span>
                    <span v-if="record.group_variable" class="parameter-pill">
                      分组：{{ record.group_variable }}
                    </span>
                  </div>
                </div>

                <div class="section-actions history-actions">
                  <button
                    type="button"
                    class="secondary-button is-compact"
                    @click="displayHistoryResult(record)"
                  >
                    查看结果
                  </button>
                  <button
                    type="button"
                    class="secondary-button is-compact"
                    @click="openReportPage(record.id, 'report')"
                  >
                    查看报告
                  </button>
                  <button
                    type="button"
                    class="secondary-button is-compact"
                    @click="openReportPage(record.id, 'script')"
                  >
                    查看脚本
                  </button>
                  <button
                    type="button"
                    class="text-button"
                    @click="openHtmlReport(record.id)"
                  >
                    HTML
                  </button>
                  <button
                    type="button"
                    class="secondary-button is-compact"
                    :disabled="rerunningRecordId === record.id"
                    @click="rerunAnalysis(record)"
                  >
                    {{ rerunningRecordId === record.id ? '重新运行中...' : '重新运行' }}
                  </button>
                </div>
              </article>
            </div>
          </article>
        </div>
      </section>
    </template>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, ref, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';

import { apiRequest, buildApiUrl, formatDateTime } from '../lib/api';

const route = useRoute();
const router = useRouter();

const datasetId = computed(() => String(route.params.datasetId ?? ''));

const datasetDetail = ref(null);
const datasetProfile = ref(null);
const analysisHistory = ref([]);
const currentTask = ref(null);
const displayResult = ref(null);
const displayRecordId = ref('');

const pageError = ref('');
const historyError = ref('');
const analysisErrorMessage = ref('');
const analysisSuccessMessage = ref('');

const detailLoading = ref(false);
const profileLoading = ref(false);
const historyLoading = ref(false);
const submittingAnalysis = ref(false);
const rerunningRecordId = ref('');

const analysisType = ref('descriptive_statistics');
const selectedVariables = ref([]);
const groupVariable = ref('');

const pollingTimerId = ref(null);

const statusLabelMap = {
  draft: '草稿',
  processing: '处理中',
  ready: '可用',
  failed: '失败',
};

const taskStatusLabelMap = {
  pending: '等待中',
  running: '运行中',
  completed: '已完成',
  failed: '失败',
};

const analysisTypeLabelMap = {
  descriptive_statistics: '描述统计',
  independent_samples_t_test: '独立样本 t 检验',
  one_way_anova: '单因素方差分析',
  chi_square_test: '卡方检验',
  correlation_analysis: '相关性分析',
};

const columnNames = computed(() =>
  Array.isArray(datasetProfile.value?.columns)
    ? datasetProfile.value.columns.map((item) => item.name)
    : [],
);

const requiresGroupVariable = computed(() =>
  ['independent_samples_t_test', 'one_way_anova'].includes(analysisType.value),
);

const analysisHelpText = computed(() => {
  if (analysisType.value === 'descriptive_statistics') {
    return '描述统计至少选择 1 个字段，适合快速查看均值、标准差、极值等概况。';
  }
  if (analysisType.value === 'correlation_analysis') {
    return '相关性分析至少选择 2 个字段，系统会计算字段间相关关系。';
  }
  if (analysisType.value === 'independent_samples_t_test') {
    return '独立样本 t 检验需要 1 个分析变量和 1 个分组变量。';
  }
  if (analysisType.value === 'one_way_anova') {
    return '单因素方差分析需要 1 个分析变量和 1 个分组变量。';
  }
  return '卡方检验通常选择 2 个分类变量，用于检验它们是否相关。';
});

const canSubmitAnalysis = computed(() => {
  if (analysisType.value === 'descriptive_statistics') {
    return selectedVariables.value.length >= 1;
  }
  if (analysisType.value === 'correlation_analysis') {
    return selectedVariables.value.length >= 2;
  }
  if (analysisType.value === 'chi_square_test') {
    return selectedVariables.value.length === 2;
  }
  return selectedVariables.value.length === 1 && Boolean(groupVariable.value);
});

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

  try {
    datasetProfile.value = await apiRequest(`/datasets/${datasetId.value}/profile`);
  } catch (error) {
    pageError.value = error instanceof Error ? error.message : '字段信息读取失败。';
  } finally {
    profileLoading.value = false;
  }
}

async function loadAnalysisHistory() {
  historyLoading.value = true;
  historyError.value = '';

  try {
    const payload = await apiRequest(`/datasets/${datasetId.value}/analysis-records`);
    analysisHistory.value = Array.isArray(payload.items) ? payload.items : [];

    if (!displayResult.value && analysisHistory.value.length) {
      displayResult.value = analysisHistory.value[0].result;
      displayRecordId.value = analysisHistory.value[0].id;
    }
  } catch (error) {
    historyError.value = error instanceof Error ? error.message : '分析历史读取失败。';
  } finally {
    historyLoading.value = false;
  }
}

function clearPollingTimer() {
  if (pollingTimerId.value) {
    window.clearTimeout(pollingTimerId.value);
    pollingTimerId.value = null;
  }
}

async function pollTask(taskId) {
  clearPollingTimer();

  try {
    const payload = await apiRequest(`/tasks/${taskId}`);
    currentTask.value = payload;

    if (payload.status === 'completed') {
      displayResult.value = payload.result ?? null;
      displayRecordId.value = payload.result?.analysis_record_id ?? '';
      analysisSuccessMessage.value = '分析任务已完成。';
      await loadAnalysisHistory();
      return;
    }

    if (payload.status === 'failed') {
      analysisErrorMessage.value = payload.error_message || '分析任务执行失败。';
      return;
    }

    pollingTimerId.value = window.setTimeout(() => {
      pollTask(taskId);
    }, 1500);
  } catch (error) {
    analysisErrorMessage.value = error instanceof Error ? error.message : '任务状态读取失败。';
  }
}

function toggleVariable(column) {
  const currentSet = new Set(selectedVariables.value);

  if (currentSet.has(column)) {
    currentSet.delete(column);
  } else {
    if (analysisType.value === 'chi_square_test' && currentSet.size >= 2) {
      const values = Array.from(currentSet);
      values.shift();
      selectedVariables.value = [...values, column];
      return;
    }

    if (['independent_samples_t_test', 'one_way_anova'].includes(analysisType.value)) {
      selectedVariables.value = [column];
      return;
    }

    currentSet.add(column);
  }

  selectedVariables.value = Array.from(currentSet);
}

function resetAnalysisForm() {
  analysisType.value = 'descriptive_statistics';
  selectedVariables.value = [];
  groupVariable.value = '';
  analysisErrorMessage.value = '';
  analysisSuccessMessage.value = '';
}

async function submitAnalysis() {
  if (!canSubmitAnalysis.value) {
    analysisErrorMessage.value = '请先把当前分析所需的变量选择完整。';
    return;
  }

  submittingAnalysis.value = true;
  analysisErrorMessage.value = '';
  analysisSuccessMessage.value = '';

  try {
    const payload = await apiRequest(`/datasets/${datasetId.value}/analysis-jobs`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        analysis_type: analysisType.value,
        variables: selectedVariables.value,
        group_variable: groupVariable.value || null,
        options: {},
      }),
    });

    currentTask.value = payload;
    analysisSuccessMessage.value = '分析任务已创建，正在后台执行。';
    await pollTask(payload.id);
  } catch (error) {
    analysisErrorMessage.value = error instanceof Error ? error.message : '分析任务创建失败。';
  } finally {
    submittingAnalysis.value = false;
  }
}

function displayHistoryResult(record) {
  displayResult.value = record.result;
  displayRecordId.value = record.id;
  analysisSuccessMessage.value = '已切换到历史分析结果。';
  analysisErrorMessage.value = '';
}

async function rerunAnalysis(record) {
  rerunningRecordId.value = record.id;
  analysisErrorMessage.value = '';
  analysisSuccessMessage.value = '';

  try {
    const payload = await apiRequest(
      `/datasets/${datasetId.value}/analysis-records/${record.id}/rerun`,
      { method: 'POST' },
    );
    currentTask.value = payload;
    analysisSuccessMessage.value = '已基于历史记录重新发起分析任务。';
    await pollTask(payload.id);
  } catch (error) {
    analysisErrorMessage.value = error instanceof Error ? error.message : '重新运行分析失败。';
  } finally {
    rerunningRecordId.value = '';
  }
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

function formatVariableList(variables) {
  if (!Array.isArray(variables) || !variables.length) {
    return '—';
  }

  return variables.join('，');
}

async function copyText(content, successMessage) {
  if (!content || !navigator.clipboard) {
    return;
  }

  try {
    await navigator.clipboard.writeText(content);
    analysisSuccessMessage.value = successMessage;
  } catch (error) {
    analysisErrorMessage.value = error instanceof Error ? error.message : '复制失败。';
  }
}

function openReportPage(recordId, panel = 'report') {
  if (!recordId) {
    return;
  }

  router.push({
    name: 'dataset-report',
    params: { datasetId: datasetId.value },
    query: {
      recordId,
      template: 'general',
      panel,
    },
  });
}

function openHtmlReport(recordId) {
  if (!recordId) {
    return;
  }

  const reportUrl = buildApiUrl(
    `/datasets/${datasetId.value}/analysis-records/${recordId}/report-html?template_key=general`,
  );
  window.open(reportUrl, '_blank', 'noopener');
}

async function loadPage() {
  pageError.value = '';
  currentTask.value = null;
  displayResult.value = null;
  displayRecordId.value = '';
  clearPollingTimer();

  await loadDatasetDetail();
  if (pageError.value) {
    return;
  }

  await Promise.all([
    loadDatasetProfile(),
    loadAnalysisHistory(),
  ]);
}

watch(
  analysisType,
  () => {
    if (analysisType.value === 'chi_square_test' && selectedVariables.value.length > 2) {
      selectedVariables.value = selectedVariables.value.slice(-2);
    }

    if (['independent_samples_t_test', 'one_way_anova'].includes(analysisType.value)) {
      selectedVariables.value = selectedVariables.value.slice(-1);
    }

    if (!requiresGroupVariable.value) {
      groupVariable.value = '';
    }
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

onBeforeUnmount(() => {
  clearPollingTimer();
});
</script>
