<template>
  <div class="dataset-cleaning-page">
    <section class="detail-topbar">
      <div class="crumb-row">
        <RouterLink to="/" class="text-link">数据集列表</RouterLink>
        <span class="crumb-separator">/</span>
        <RouterLink :to="{ name: 'dataset-detail', params: { datasetId } }" class="text-link">
          预览
        </RouterLink>
        <span class="crumb-separator">/</span>
        <span class="crumb-current">清洗</span>
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
            <p class="section-label">数据清洗</p>
            <span v-if="datasetDetail" class="status-badge" :class="`status-${datasetDetail.status}`">
              {{ statusLabelMap[datasetDetail.status] ?? datasetDetail.status }}
            </span>
          </div>
          <p class="detail-dataset-name">{{ datasetDetail?.name ?? '正在读取数据集信息...' }}</p>
          <p class="detail-file-name">当前新增的清洗步骤会直接影响预览、分析和脚本输出。</p>
        </div>

        <div class="detail-metric-row">
          <div class="metric-chip"><span>步骤数</span><strong>{{ cleaningSteps.length }}</strong></div>
          <div class="metric-chip"><span>字段数</span><strong>{{ columnNames.length }}</strong></div>
          <div class="metric-chip"><span>脚本</span><strong>{{ cleaningScript ? '已生成' : '未读取' }}</strong></div>
          <div class="metric-chip"><span>文件</span><strong>{{ datasetDetail?.file_name ?? '-' }}</strong></div>
        </div>
      </section>

      <section class="cleaning-layout">
        <aside class="cleaning-sidebar">
          <article class="panel-card form-card">
            <div class="section-head">
              <div>
                <p class="section-label">新增步骤</p>
                <p class="section-title">配置清洗规则</p>
              </div>
            </div>

            <div class="form-grid">
              <label class="field-block">
                <span class="field-label">步骤类型</span>
                <select v-model="stepType" class="form-control">
                  <option value="filter">筛选</option>
                  <option value="missing_value">缺失值处理</option>
                  <option value="sort">排序</option>
                  <option value="recode">重编码</option>
                  <option value="derive_variable">新变量</option>
                </select>
              </label>

              <label class="field-block">
                <span class="field-label">步骤名称</span>
                <input v-model="stepName" type="text" class="form-control" placeholder="可留空，系统会自动命名" />
              </label>

              <label class="field-block">
                <span class="field-label">说明</span>
                <textarea v-model="stepDescription" class="form-control form-textarea" placeholder="这一步的用途"></textarea>
              </label>

              <label class="checkbox-row">
                <input v-model="stepEnabled" type="checkbox" />
                <span>创建后立即启用</span>
              </label>

              <template v-if="stepType === 'filter'">
                <div class="field-grid">
                  <label class="field-block">
                    <span class="field-label">字段</span>
                    <select v-model="filterColumn" class="form-control">
                      <option value="">请选择字段</option>
                      <option v-for="column in columnNames" :key="column" :value="column">{{ column }}</option>
                    </select>
                  </label>
                  <label class="field-block">
                    <span class="field-label">操作符</span>
                    <select v-model="filterOperator" class="form-control">
                      <option value="eq">等于</option>
                      <option value="neq">不等于</option>
                      <option value="contains">包含</option>
                      <option value="gt">大于</option>
                      <option value="gte">大于等于</option>
                      <option value="lt">小于</option>
                      <option value="lte">小于等于</option>
                      <option value="between">区间</option>
                      <option value="is_empty">为空</option>
                      <option value="is_not_empty">非空</option>
                    </select>
                  </label>
                </div>
                <div v-if="filterOperator === 'between'" class="field-grid">
                  <label class="field-block">
                    <span class="field-label">起始值</span>
                    <input v-model="filterStart" type="text" class="form-control" />
                  </label>
                  <label class="field-block">
                    <span class="field-label">结束值</span>
                    <input v-model="filterEnd" type="text" class="form-control" />
                  </label>
                </div>
                <label v-else-if="!['is_empty', 'is_not_empty'].includes(filterOperator)" class="field-block">
                  <span class="field-label">值</span>
                  <input v-model="filterValue" type="text" class="form-control" />
                </label>
              </template>

              <template v-if="stepType === 'missing_value'">
                <label class="field-block">
                  <span class="field-label">处理方式</span>
                  <select v-model="missingMethod" class="form-control">
                    <option value="drop_rows">删除含缺失值的整行</option>
                    <option value="fill_value">用指定值填充</option>
                    <option value="mark_values">把指定值标记为缺失</option>
                  </select>
                </label>
                <label v-if="missingMethod !== 'drop_rows'" class="field-block">
                  <span class="field-label">字段</span>
                  <select v-model="missingColumn" class="form-control">
                    <option value="">请选择字段</option>
                    <option v-for="column in columnNames" :key="column" :value="column">{{ column }}</option>
                  </select>
                </label>
                <label v-if="missingMethod === 'fill_value'" class="field-block">
                  <span class="field-label">填充值</span>
                  <input v-model="missingValue" type="text" class="form-control" />
                </label>
                <label v-if="missingMethod === 'mark_values'" class="field-block">
                  <span class="field-label">待标记值</span>
                  <textarea v-model="missingValuesText" class="form-control form-textarea" placeholder="可用逗号、空格或换行分隔"></textarea>
                </label>
              </template>

              <template v-if="stepType === 'sort'">
                <div class="field-grid">
                  <label class="field-block">
                    <span class="field-label">字段</span>
                    <select v-model="sortColumn" class="form-control">
                      <option value="">请选择字段</option>
                      <option v-for="column in columnNames" :key="column" :value="column">{{ column }}</option>
                    </select>
                  </label>
                  <label class="field-block">
                    <span class="field-label">顺序</span>
                    <select v-model="sortDirection" class="form-control">
                      <option value="asc">升序</option>
                      <option value="desc">降序</option>
                    </select>
                  </label>
                </div>
              </template>

              <template v-if="stepType === 'recode'">
                <label class="field-block">
                  <span class="field-label">字段</span>
                  <select v-model="recodeColumn" class="form-control">
                    <option value="">请选择字段</option>
                    <option v-for="column in columnNames" :key="column" :value="column">{{ column }}</option>
                  </select>
                </label>
                <label class="field-block">
                  <span class="field-label">映射规则</span>
                  <textarea v-model="recodeMappingText" class="form-control form-textarea" placeholder="每行一条，例如：&#10;男 => 1&#10;女 => 2"></textarea>
                </label>
              </template>

              <template v-if="stepType === 'derive_variable'">
                <label class="field-block">
                  <span class="field-label">生成方式</span>
                  <select v-model="deriveMethod" class="form-control">
                    <option value="binary_operation">双字段数值运算</option>
                    <option value="concat">字段拼接</option>
                  </select>
                </label>
                <label class="field-block">
                  <span class="field-label">新字段名</span>
                  <input v-model="deriveNewColumn" type="text" class="form-control" />
                </label>
                <div v-if="deriveMethod === 'binary_operation'" class="field-grid">
                  <label class="field-block">
                    <span class="field-label">左字段</span>
                    <select v-model="deriveLeftColumn" class="form-control">
                      <option value="">请选择字段</option>
                      <option v-for="column in columnNames" :key="column" :value="column">{{ column }}</option>
                    </select>
                  </label>
                  <label class="field-block">
                    <span class="field-label">右字段</span>
                    <select v-model="deriveRightColumn" class="form-control">
                      <option value="">请选择字段</option>
                      <option v-for="column in columnNames" :key="column" :value="column">{{ column }}</option>
                    </select>
                  </label>
                  <label class="field-block field-block-full">
                    <span class="field-label">运算符</span>
                    <select v-model="deriveOperator" class="form-control">
                      <option value="add">相加</option>
                      <option value="subtract">相减</option>
                      <option value="multiply">相乘</option>
                      <option value="divide">相除</option>
                    </select>
                  </label>
                </div>
                <template v-else>
                  <label class="field-block">
                    <span class="field-label">来源字段</span>
                    <textarea v-model="deriveSourceColumnsText" class="form-control form-textarea" placeholder="多个字段可用逗号、空格或换行分隔"></textarea>
                  </label>
                  <label class="field-block">
                    <span class="field-label">分隔符</span>
                    <input v-model="deriveSeparator" type="text" class="form-control" placeholder="例如：-" />
                  </label>
                </template>
              </template>
            </div>

            <div class="button-row">
              <button type="button" class="primary-button" :disabled="creatingStep || !canSubmit" @click="submitCleaningStep">
                {{ creatingStep ? '保存中...' : '保存清洗步骤' }}
              </button>
              <button type="button" class="secondary-button" :disabled="creatingStep" @click="resetForm()">
                重置
              </button>
            </div>

            <p v-if="createSuccessMessage" class="feedback success-text">{{ createSuccessMessage }}</p>
            <p v-if="createErrorMessage" class="feedback error-text">{{ createErrorMessage }}</p>
          </article>

          <article class="panel-card info-card">
            <div class="section-head">
              <div>
                <p class="section-label">字段参考</p>
                <p class="section-title">当前可用字段</p>
              </div>
              <button type="button" class="text-button" :disabled="profileLoading" @click="loadDatasetProfile">
                {{ profileLoading ? '读取中...' : '刷新' }}
              </button>
            </div>

            <div v-if="profileLoading && !columnNames.length" class="empty-state">
              正在读取字段...
            </div>
            <div v-else-if="!columnNames.length" class="empty-state">
              <p>当前没有字段可用。</p>
            </div>
            <div v-else class="column-pills">
              <span v-for="column in columnNames" :key="column" class="column-pill">{{ column }}</span>
            </div>
          </article>
        </aside>

        <div class="cleaning-main">
          <article class="panel-card">
            <div class="section-head">
              <div>
                <p class="section-label">步骤列表</p>
                <p class="section-title">当前清洗流程</p>
              </div>
              <button type="button" class="text-button" :disabled="stepsLoading" @click="loadCleaningSteps">
                {{ stepsLoading ? '读取中...' : '刷新' }}
              </button>
            </div>

            <p v-if="stepsError" class="feedback error-text">{{ stepsError }}</p>
            <div v-if="stepsLoading && !cleaningSteps.length" class="empty-state">
              正在读取清洗步骤...
            </div>
            <div v-else-if="!cleaningSteps.length" class="empty-state">
              <p>当前还没有清洗步骤。你可以先在左侧添加第一条规则。</p>
            </div>
            <div v-else class="step-list">
              <article v-for="step in cleaningSteps" :key="step.id" class="step-card">
                <div class="step-card-head">
                  <div>
                    <p class="step-order">步骤 {{ step.order }}</p>
                    <h3 class="step-name">{{ step.name }}</h3>
                  </div>
                  <div class="step-badges">
                    <span class="step-type-badge">{{ stepTypeLabelMap[step.step_type] ?? step.step_type }}</span>
                    <span class="status-badge" :class="step.enabled ? 'status-ready' : 'status-draft'">
                      {{ step.enabled ? '启用' : '禁用' }}
                    </span>
                  </div>
                </div>
                <p v-if="step.description" class="step-description">{{ step.description }}</p>
                <div class="parameter-list">
                  <span v-for="item in describeStep(step)" :key="item" class="parameter-pill">{{ item }}</span>
                </div>
                <p class="step-created-at">创建于 {{ formatDateTime(step.created_at) }}</p>
              </article>
            </div>
          </article>

          <article class="panel-card">
            <div class="section-head">
              <div>
                <p class="section-label">R 脚本</p>
                <p class="section-title">当前清洗脚本草稿</p>
              </div>
              <div class="section-actions">
                <button type="button" class="text-button" :disabled="scriptLoading" @click="loadCleaningScript">
                  {{ scriptLoading ? '读取中...' : '刷新' }}
                </button>
                <button type="button" class="text-button" :disabled="!cleaningScript?.script" @click="copyScript">
                  复制
                </button>
              </div>
            </div>

            <p v-if="scriptError" class="feedback error-text">{{ scriptError }}</p>
            <div v-if="scriptLoading && !cleaningScript" class="empty-state">
              正在读取清洗脚本...
            </div>
            <div v-else-if="!cleaningScript?.script" class="empty-state">
              <p>当前还没有可展示的清洗脚本。</p>
            </div>
            <div v-else class="script-panel">
              <div class="script-meta">
                <span>文件：{{ cleaningScript.file_name }}</span>
                <span>步骤数：{{ cleaningScript.step_count }}</span>
              </div>
              <pre class="script-block"><code>{{ cleaningScript.script }}</code></pre>
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

import { apiRequest, formatDateTime } from '../lib/api';

const route = useRoute();

const datasetId = computed(() => String(route.params.datasetId ?? ''));
const datasetDetail = ref(null);
const datasetProfile = ref(null);
const cleaningSteps = ref([]);
const cleaningScript = ref(null);

const pageError = ref('');
const stepsError = ref('');
const scriptError = ref('');
const createErrorMessage = ref('');
const createSuccessMessage = ref('');

const detailLoading = ref(false);
const profileLoading = ref(false);
const stepsLoading = ref(false);
const scriptLoading = ref(false);
const creatingStep = ref(false);

const stepType = ref('filter');
const stepName = ref('');
const stepDescription = ref('');
const stepEnabled = ref(true);
const filterColumn = ref('');
const filterOperator = ref('eq');
const filterValue = ref('');
const filterStart = ref('');
const filterEnd = ref('');
const missingMethod = ref('drop_rows');
const missingColumn = ref('');
const missingValue = ref('');
const missingValuesText = ref('');
const sortColumn = ref('');
const sortDirection = ref('asc');
const recodeColumn = ref('');
const recodeMappingText = ref('');
const deriveMethod = ref('binary_operation');
const deriveNewColumn = ref('');
const deriveLeftColumn = ref('');
const deriveRightColumn = ref('');
const deriveOperator = ref('add');
const deriveSourceColumnsText = ref('');
const deriveSeparator = ref('');

const statusLabelMap = { draft: '草稿', processing: '处理中', ready: '可用', failed: '失败' };
const stepTypeLabelMap = {
  filter: '筛选',
  missing_value: '缺失值',
  sort: '排序',
  recode: '重编码',
  derive_variable: '新变量',
};
const filterOperatorLabelMap = {
  eq: '等于',
  neq: '不等于',
  contains: '包含',
  gt: '大于',
  gte: '大于等于',
  lt: '小于',
  lte: '小于等于',
  between: '区间',
  is_empty: '为空',
  is_not_empty: '非空',
};
const deriveOperatorLabelMap = { add: '+', subtract: '-', multiply: '*', divide: '/' };

const columnNames = computed(() =>
  Array.isArray(datasetProfile.value?.columns)
    ? datasetProfile.value.columns.map((item) => item.name)
    : [],
);

const canSubmit = computed(() => {
  if (!datasetId.value) return false;
  if (stepType.value === 'filter') {
    if (!filterColumn.value) return false;
    if (filterOperator.value === 'between') return Boolean(filterStart.value.trim() && filterEnd.value.trim());
    if (['is_empty', 'is_not_empty'].includes(filterOperator.value)) return true;
    return Boolean(filterValue.value.trim());
  }
  if (stepType.value === 'missing_value') {
    if (missingMethod.value === 'drop_rows') return true;
    if (missingMethod.value === 'fill_value') return Boolean(missingColumn.value && missingValue.value.trim());
    return Boolean(missingColumn.value && parseTextList(missingValuesText.value).length);
  }
  if (stepType.value === 'sort') return Boolean(sortColumn.value);
  if (stepType.value === 'recode') return Boolean(recodeColumn.value && parseMappingText(recodeMappingText.value));
  if (!deriveNewColumn.value.trim()) return false;
  if (deriveMethod.value === 'binary_operation') return Boolean(deriveLeftColumn.value && deriveRightColumn.value);
  return parseTextList(deriveSourceColumnsText.value).length > 0;
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

async function loadCleaningSteps() {
  stepsLoading.value = true;
  stepsError.value = '';
  try {
    const payload = await apiRequest(`/datasets/${datasetId.value}/cleaning-steps`);
    cleaningSteps.value = Array.isArray(payload.items) ? payload.items : [];
  } catch (error) {
    stepsError.value = error instanceof Error ? error.message : '清洗步骤读取失败。';
  } finally {
    stepsLoading.value = false;
  }
}

async function loadCleaningScript() {
  scriptLoading.value = true;
  scriptError.value = '';
  try {
    cleaningScript.value = await apiRequest(`/datasets/${datasetId.value}/cleaning-r-script`);
  } catch (error) {
    scriptError.value = error instanceof Error ? error.message : '清洗脚本读取失败。';
  } finally {
    scriptLoading.value = false;
  }
}

async function loadPage() {
  pageError.value = '';
  await loadDatasetDetail();
  if (pageError.value) return;
  await Promise.all([loadDatasetProfile(), loadCleaningSteps(), loadCleaningScript()]);
}

function parseTextList(rawText) {
  return rawText.split(/[\n,，\s]+/).map((item) => item.trim()).filter(Boolean);
}

function parseMappingText(rawText) {
  const lines = rawText.split('\n').map((line) => line.trim()).filter(Boolean);
  if (!lines.length) return null;
  const mapping = {};
  for (const line of lines) {
    const separator = line.includes('=>') ? '=>' : '=';
    if (!line.includes(separator)) return null;
    const [source, ...rest] = line.split(separator);
    const target = rest.join(separator);
    if (!source.trim() || !target.trim()) return null;
    mapping[source.trim()] = target.trim();
  }
  return Object.keys(mapping).length ? mapping : null;
}

function buildPayload() {
  let parameters = {};
  if (stepType.value === 'filter') {
    parameters = { column: filterColumn.value, operator: filterOperator.value };
    if (filterOperator.value === 'between') {
      parameters.start = filterStart.value.trim();
      parameters.end = filterEnd.value.trim();
    } else if (!['is_empty', 'is_not_empty'].includes(filterOperator.value)) {
      parameters.value = filterValue.value.trim();
    }
  }
  if (stepType.value === 'missing_value') {
    parameters = { method: missingMethod.value };
    if (missingMethod.value === 'fill_value') {
      parameters.column = missingColumn.value;
      parameters.value = missingValue.value.trim();
    }
    if (missingMethod.value === 'mark_values') {
      parameters.column = missingColumn.value;
      parameters.values = parseTextList(missingValuesText.value);
    }
  }
  if (stepType.value === 'sort') {
    parameters = { column: sortColumn.value, direction: sortDirection.value };
  }
  if (stepType.value === 'recode') {
    parameters = { column: recodeColumn.value, mapping: parseMappingText(recodeMappingText.value) };
  }
  if (stepType.value === 'derive_variable') {
    parameters = { method: deriveMethod.value, new_column: deriveNewColumn.value.trim() };
    if (deriveMethod.value === 'binary_operation') {
      parameters.left_column = deriveLeftColumn.value;
      parameters.right_column = deriveRightColumn.value;
      parameters.operator = deriveOperator.value;
    } else {
      parameters.source_columns = parseTextList(deriveSourceColumnsText.value);
      parameters.separator = deriveSeparator.value;
    }
  }
  return {
    step_type: stepType.value,
    name: stepName.value.trim() || buildDefaultStepName(),
    description: stepDescription.value.trim() || null,
    enabled: stepEnabled.value,
    parameters,
  };
}

function buildDefaultStepName() {
  if (stepType.value === 'filter') return `筛选 ${filterColumn.value || '字段'}`;
  if (stepType.value === 'missing_value') {
    if (missingMethod.value === 'drop_rows') return '删除缺失值行';
    if (missingMethod.value === 'fill_value') return `填充 ${missingColumn.value || '字段'}`;
    return `标记 ${missingColumn.value || '字段'} 缺失值`;
  }
  if (stepType.value === 'sort') return `排序 ${sortColumn.value || '字段'}`;
  if (stepType.value === 'recode') return `重编码 ${recodeColumn.value || '字段'}`;
  return `生成 ${deriveNewColumn.value || '新字段'}`;
}

async function submitCleaningStep() {
  if (!canSubmit.value) {
    createErrorMessage.value = '请先把当前步骤需要的字段填完整。';
    return;
  }
  creatingStep.value = true;
  createErrorMessage.value = '';
  createSuccessMessage.value = '';
  try {
    const payload = buildPayload();
    const createdStep = await apiRequest(`/datasets/${datasetId.value}/cleaning-steps`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    createSuccessMessage.value = `步骤 ${createdStep.name} 已保存。`;
    resetForm(true);
    await Promise.all([loadCleaningSteps(), loadCleaningScript(), loadDatasetProfile()]);
  } catch (error) {
    createErrorMessage.value = error instanceof Error ? error.message : '清洗步骤保存失败。';
  } finally {
    creatingStep.value = false;
  }
}

function resetForm(preserveType = false) {
  stepName.value = '';
  stepDescription.value = '';
  stepEnabled.value = true;
  if (!preserveType) stepType.value = 'filter';
  filterColumn.value = '';
  filterOperator.value = 'eq';
  filterValue.value = '';
  filterStart.value = '';
  filterEnd.value = '';
  missingMethod.value = 'drop_rows';
  missingColumn.value = '';
  missingValue.value = '';
  missingValuesText.value = '';
  sortColumn.value = '';
  sortDirection.value = 'asc';
  recodeColumn.value = '';
  recodeMappingText.value = '';
  deriveMethod.value = 'binary_operation';
  deriveNewColumn.value = '';
  deriveLeftColumn.value = '';
  deriveRightColumn.value = '';
  deriveOperator.value = 'add';
  deriveSourceColumnsText.value = '';
  deriveSeparator.value = '';
  createErrorMessage.value = '';
}

function describeStep(step) {
  const parameters = step.parameters ?? {};
  if (step.step_type === 'filter') {
    if (parameters.operator === 'between') {
      return [`字段: ${parameters.column}`, '操作: 区间', `起始: ${parameters.start}`, `结束: ${parameters.end}`];
    }
    if (['is_empty', 'is_not_empty'].includes(parameters.operator)) {
      return [`字段: ${parameters.column}`, `操作: ${filterOperatorLabelMap[parameters.operator]}`];
    }
    return [`字段: ${parameters.column}`, `操作: ${filterOperatorLabelMap[parameters.operator] ?? parameters.operator}`, `值: ${parameters.value}`];
  }
  if (step.step_type === 'missing_value') {
    if (parameters.method === 'drop_rows') return ['方式: 删除含缺失值的整行'];
    if (parameters.method === 'fill_value') return [`字段: ${parameters.column}`, '方式: 填充', `值: ${parameters.value}`];
    return [`字段: ${parameters.column}`, '方式: 标记为缺失', `值: ${(parameters.values ?? []).join('，')}`];
  }
  if (step.step_type === 'sort') return [`字段: ${parameters.column}`, `顺序: ${parameters.direction === 'desc' ? '降序' : '升序'}`];
  if (step.step_type === 'recode') return [`字段: ${parameters.column}`, `映射数: ${Object.keys(parameters.mapping ?? {}).length}`];
  if (parameters.method === 'binary_operation') {
    return [`新字段: ${parameters.new_column}`, `${parameters.left_column} ${deriveOperatorLabelMap[parameters.operator] ?? parameters.operator} ${parameters.right_column}`];
  }
  return [`新字段: ${parameters.new_column}`, `来源: ${(parameters.source_columns ?? []).join('，')}`, `分隔符: ${parameters.separator || '(空)'}`];
}

async function copyScript() {
  if (!cleaningScript.value?.script || !navigator.clipboard) return;
  try {
    await navigator.clipboard.writeText(cleaningScript.value.script);
    createSuccessMessage.value = '清洗脚本已复制到剪贴板。';
  } catch (error) {
    createErrorMessage.value = error instanceof Error ? error.message : '复制脚本失败。';
  }
}

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
</script>
