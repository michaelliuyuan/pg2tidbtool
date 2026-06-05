import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      redirect: '/wizard',
    },
    {
      path: '/wizard',
      name: 'Wizard',
      component: () => import('../views/WizardView.vue'),
    },
    {
      path: '/tasks',
      name: 'TaskList',
      component: () => import('../views/TaskListView.vue'),
    },
    {
      path: '/tasks/:id',
      name: 'TaskDetail',
      component: () => import('../views/TaskDetailView.vue'),
    },
    {
      path: '/history',
      name: 'History',
      component: () => import('../views/HistoryView.vue'),
    },
    {
      path: '/assess',
      name: 'Assess',
      component: () => import('../views/AssessView.vue'),
    },
  ],
})

export default router
