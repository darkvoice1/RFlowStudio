import { createRouter, createWebHistory } from 'vue-router';
import DatasetHomeView from '../views/DatasetHomeView.vue';
import DatasetDetailView from '../views/DatasetDetailView.vue';
import DatasetCleaningView from '../views/DatasetCleaningView.vue';

const routes = [
  {
    path: '/',
    name: 'dataset-home',
    component: DatasetHomeView,
  },
  {
    path: '/datasets/:datasetId',
    name: 'dataset-detail',
    component: DatasetDetailView,
  },
  {
    path: '/datasets/:datasetId/cleaning',
    name: 'dataset-cleaning',
    component: DatasetCleaningView,
  },
];

const router = createRouter({
  history: createWebHistory(),
  routes,
});

export default router;
