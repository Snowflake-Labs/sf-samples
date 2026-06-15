# SSP 360 frontend

Supply-side / AdTech identity graph UI. Runs separately from the Travel use-case app so you can develop both without port clashes.

## Dev server

```bash
npm install
npm run dev
```

Default URL: **http://localhost:5175** (set in `vite.config.ts`).

Proxy: `/api` → `http://localhost:8001` (adjust if your FastAPI port differs).

Set `VITE_INDUSTRY=SSP` and any other env vars in `.env` as needed for your backend.
