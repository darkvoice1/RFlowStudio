<template>
  <div class="dataset-page">
    <section class="workspace-strip">
      <div class="workspace-strip-copy">
        <p class="section-label">工作台</p>
        <p class="workspace-strip-text">
          先从这里上传数据集。后面的预览、清洗、分析、脚本和报告都会围绕同一份数据继续展开。
        </p>
      </div>

      <div class="workspace-strip-stats">
        <div class="mini-stat">
          <span>已有</span>
          <strong>{{ datasets.length }}</strong>
        </div>
        <div class="mini-stat">
          <span>可用</span>
          <strong>{{ readyDatasetCount }}</strong>
        </div>
        <div class="mini-stat">
          <span>处理中</span>
          <strong>{{ processingDatasetCount }}</strong>
        </div>
      </div>
    </section>

    <section class="content-grid">
      <article class="panel-card">
        <div class="section-head">
          <div>
            <p class="section-label">上传</p>
            <p class="section-title">新建数据集</p>
          </div>
          <button
            type="button"
            class="text-button"
            :disabled="capabilitiesLoading"
            @click="loadUploadCapabilities"
          >
            {{ capabilitiesLoading ? '读取中...' : '刷新规则' }}
          </button>
        </div>

        <p v-if="uploadCapabilities" class="section-copy">
          支持 {{ uploadCapabilities.supported_extensions.join(' / ') }}，单文件不超过
          {{ uploadCapabilities.max_file_size_mb }} MB。
        </p>
        <p v-else class="section-copy">
          正在读取上传规则...
        </p>

        <label class="upload-box" :class="{ 'is-busy': uploading }">
          <input
            ref="fileInputRef"
            type="file"
            class="sr-only"
            :accept="acceptedFileTypes"
            :disabled="uploading"
            @change="handleFileSelection"
          />
          <span class="upload-box-title">
            {{ selectedFile ? selectedFile.name : '选择一个数据文件' }}
          </span>
          <span class="upload-box-copy">
            {{
              selectedFile
                ? `文件大小：${formatFileSize(selectedFile.size)}`
                : '支持 CSV、XLSX、SAV。建议先从一份小型数据开始验证流程。'
            }}
          </span>
        </label>

        <div class="button-row">
          <button
            type="button"
            class="primary-button"
            :disabled="uploading || !selectedFile"
            @click="submitUpload"
          >
            {{ uploading ? '上传中...' : '上传并创建数据集' }}
          </button>
          <button
            type="button"
            class="secondary-button"
            :disabled="uploading || !selectedFile"
            @click="clearSelectedFile"
          >
            清空
          </button>
        </div>

        <p v-if="uploadMessage" class="feedback success-text">
          {{ uploadMessage }}
        </p>
        <p v-if="uploadError" class="feedback error-text">
          {{ uploadError }}
        </p>
      </article>

      <article class="panel-card">
        <div class="section-head">
          <div>
            <p class="section-label">列表</p>
            <p class="section-title">已有数据集</p>
          </div>
          <button
            type="button"
            class="text-button"
            :disabled="datasetsLoading"
            @click="loadDatasets"
          >
            {{ datasetsLoading ? '刷新中...' : '刷新列表' }}
          </button>
        </div>

        <p v-if="datasetsError" class="feedback error-text">
          {{ datasetsError }}
        </p>

        <div v-if="datasetsLoading && !datasets.length" class="empty-state">
          正在读取数据集列表...
        </div>

        <div v-else-if="!datasets.length" class="empty-state">
          <p>还没有数据集。先上传第一份文件，后面的页面都会围绕它展开。</p>
        </div>

        <div v-else class="dataset-list">
          <article
            v-for="dataset in datasets"
            :key="dataset.id"
            class="dataset-item"
          >
            <div class="dataset-item-top">
              <div>
                <h4>{{ dataset.name }}</h4>
                <p>{{ dataset.file_name }}</p>
              </div>
              <span class="status-badge" :class="`status-${dataset.status}`">
                {{ statusLabelMap[dataset.status] ?? dataset.status }}
              </span>
            </div>

            <dl class="dataset-meta">
              <div>
                <dt>大小</dt>
                <dd>{{ formatFileSize(dataset.size_bytes) }}</dd>
              </div>
              <div>
                <dt>创建时间</dt>
                <dd>{{ formatDateTime(dataset.created_at) }}</dd>
              </div>
              <div>
                <dt>数据集 ID</dt>
                <dd class="dataset-id">{{ dataset.id }}</dd>
              </div>
            </dl>

            <div class="dataset-item-actions">
              <RouterLink
                :to="{ name: 'dataset-detail', params: { datasetId: dataset.id } }"
                class="secondary-button is-compact"
              >
                打开
              </RouterLink>
            </div>
          </article>
        </div>
      </article>
    </section>
  </div>
</template>

<script setup>
import { computed, onMounted, ref } from 'vue';

import { apiRequest, formatDateTime, formatFileSize } from '../lib/api';

const uploadCapabilities = ref(null);
const capabilitiesLoading = ref(false);
const datasetsLoading = ref(false);
const datasetsError = ref('');
const uploadError = ref('');
const uploadMessage = ref('');
const uploading = ref(false);
const datasets = ref([]);
const selectedFile = ref(null);
const fileInputRef = ref(null);

const statusLabelMap = {
  draft: '草稿',
  processing: '处理中',
  ready: '可用',
  failed: '失败',
};

const acceptedFileTypes = computed(() => {
  if (!uploadCapabilities.value) {
    return '.csv,.xlsx,.sav';
  }

  return uploadCapabilities.value.supported_extensions.join(',');
});

const readyDatasetCount = computed(() =>
  datasets.value.filter((item) => item.status === 'ready').length,
);

const processingDatasetCount = computed(() =>
  datasets.value.filter((item) => item.status === 'processing').length,
);

async function loadUploadCapabilities() {
  capabilitiesLoading.value = true;

  try {
    uploadCapabilities.value = await apiRequest('/datasets/upload-capabilities');
  } catch (error) {
    uploadError.value = error instanceof Error ? error.message : '上传规则读取失败。';
  } finally {
    capabilitiesLoading.value = false;
  }
}

async function loadDatasets() {
  datasetsLoading.value = true;
  datasetsError.value = '';

  try {
    const payload = await apiRequest('/datasets');
    datasets.value = Array.isArray(payload.items) ? payload.items : [];
  } catch (error) {
    datasetsError.value = error instanceof Error ? error.message : '数据集列表读取失败。';
  } finally {
    datasetsLoading.value = false;
  }
}

function handleFileSelection(event) {
  const input = event.target;
  const [file] = input.files ?? [];
  selectedFile.value = file ?? null;
  uploadError.value = '';
  uploadMessage.value = '';
}

function clearSelectedFile() {
  selectedFile.value = null;
  uploadError.value = '';
  uploadMessage.value = '';
  if (fileInputRef.value) {
    fileInputRef.value.value = '';
  }
}

async function submitUpload() {
  if (!selectedFile.value) {
    uploadError.value = '请先选择一个数据文件。';
    return;
  }

  uploading.value = true;
  uploadError.value = '';
  uploadMessage.value = '';

  const formData = new FormData();
  formData.append('file', selectedFile.value);

  try {
    const payload = await apiRequest('/datasets/upload', {
      method: 'POST',
      body: formData,
    });

    uploadMessage.value = `数据集 ${payload.name} 上传成功，当前状态：${statusLabelMap[payload.status] ?? payload.status}。`;
    clearSelectedFile();
    await loadDatasets();
  } catch (error) {
    uploadError.value = error instanceof Error ? error.message : '数据集上传失败。';
  } finally {
    uploading.value = false;
  }
}

onMounted(async () => {
  await Promise.all([loadUploadCapabilities(), loadDatasets()]);
});
</script>
