.PHONY: up down logs test test-integration produce psql

up:            ## Start the full stack (Kafka, LocalStack/SQS, Postgres, processor)
	docker compose up -d --build
	docker compose ps

down:          ## Stop everything and drop volumes
	docker compose down -v

logs:          ## Tail processor logs
	docker compose logs -f processor

produce:       ## Run the producer simulator once (valid + edge-case events)
	docker compose run --rm producer

test:          ## Unit tests — no stack required
	python -m pytest tests/unit -q

test-integration: ## End-to-end tests against the running compose stack
	python -m pytest tests/integration -q

psql:          ## Shell into the events database
	docker compose exec postgres psql -U events -d events
