.PHONY: dev-backend dev-frontend local-infra local-infra-down seed ingest

local-infra:
	docker compose -f deploy/docker-compose.yml up -d

local-infra-down:
	docker compose -f deploy/docker-compose.yml down -v

seed:
	bash deploy/seed.sh

dev-backend:
	cd backend && uvicorn api:app --reload --port 8000

dev-frontend:
	cd frontend && npm run dev

ingest:
	cd ingestion && \
	python parser.py && \
	python extract_specs.py && \
	python chunker.py && \
	python embedder.py && \
	bash ../deploy/seed.sh