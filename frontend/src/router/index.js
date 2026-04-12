import { createRouter, createWebHistory } from 'vue-router';

const routes = [
  {
    path: '/',
    name: 'home',
    component: {
      template: `
        <div style="margin-top: 20px; color: #334155;">
          Base router is ready. Feature pages will be added in later phases.
        </div>
      `,
    },
  },
];

const router = createRouter({
  history: createWebHistory(),
  routes,
});

export default router;

