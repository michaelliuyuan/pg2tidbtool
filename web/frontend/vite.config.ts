import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
      '/ws': {
        target: 'ws://localhost:8080',
        ws: true,
      },
    },
  },
  build: {
    outDir: '../dist',
    emptyOutDir: true,
    rollupOptions: {
      output: {
        // pg2tidb is an ops/management tool, not a high-traffic app, so
        // code-splitting's size benefit is marginal — and independent view
        // chunks are exactly what can 404 behind an inconsistent static-dir /
        // cache layer (the #t48 CDC blank-page failure). Bundle everything into
        // one chunk: no view chunk can be missed or negative-cached.
        inlineDynamicImports: true,
      },
    },
  },
})
