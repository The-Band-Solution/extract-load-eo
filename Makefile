# Diretório do código-fonte
SRC_DIR=src

# Alvo para rodar os testes
test:
	PYTHONPATH=$(SRC_DIR) pytest -s -v
up:
	docker compose up --build
down:
	docker compose down -v
