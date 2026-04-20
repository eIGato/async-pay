.PHONY: up down logs build restart clean ps fmt lint test install hooks

COMPOSE := docker compose

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f --tail=200

build:
	$(COMPOSE) build

restart:
	$(COMPOSE) restart

ps:
	$(COMPOSE) ps

clean:
	$(COMPOSE) down -v

install:
	pip install -e ".[dev]"

hooks:
	pre-commit install

fmt:
	ruff format .
	ruff check --fix .

lint:
	ruff check .
	ruff format --check .

test:
	pytest
