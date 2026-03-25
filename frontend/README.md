# Frontend

Next.js 15 interface for OMA-RAG.

---

## Interface

Single text input. The response streams in as it is generated. Each response is self-contained and either answers using grounded evidence with inline citations, or explicitly states that the information is not available in the corpus. Citations appear inline next to each claim as project tags. Each tag links directly to the source project on oma.com. The interface is fully responsive.

---

## Environment Variables

```bash
API_URL=http://localhost:8000
```

---

## Run

```bash
npm install  # first time only
npm run dev  # http://localhost:3000
```

---

## Build

```bash
npm run build
npm start
```

Deployable to Vercel, Netlify, or any SSR-compatible host.