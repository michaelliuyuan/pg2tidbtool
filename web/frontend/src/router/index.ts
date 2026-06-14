import { createRouter, createWebHistory } from 'vue-router'
import CDCView from '../views/CDCView.vue'

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
    {
      path: '/cdc',
      name: 'CDC',
      // Static import (not lazy) so CDCView is bundled into the main chunk — a
      // separate CDCView-*.js chunk can 404 behind an inconsistent static-dir /
      // cache layer and leave the route blank. See #t48.
      component: CDCView,
    },
  ],
})

export default router
