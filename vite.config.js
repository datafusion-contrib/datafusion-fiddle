import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { fileURLToPath, URL } from 'node:url'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  assetsInclude: ['**/*.toml'],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./', import.meta.url)),
    },
  },
  server: {
    port: 3000,
    // `vercel dev` cannot serve the official @vercel/rust runtime locally, so for
    // local dev run the function binary separately (`pnpm dev:api`) and proxy /api
    // to it. The binary is a plain HTTP server on VERCEL_DEV_PORT (see vercel_runtime).
    proxy: {
      '/api': 'http://127.0.0.1:3141',
    },
  },
})