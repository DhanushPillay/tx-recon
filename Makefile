.PHONY: up down install clean format

# Setup virtual environment and install dependencies
install:
	python -m venv .venv
	.venv\Scripts\pip install -r requirements.txt

# Start infrastructure
up:
	docker-compose up -d

# Stop infrastructure
down:
	docker-compose down -v

# Clean compiled python files
clean:
	Get-ChildItem -Path . -Include __pycache__ -Recurse -Directory | Remove-Item -Recurse -Force
	Get-ChildItem -Path . -Include *.pyc -Recurse -File | Remove-Item -Force

# Format code
format:
	.venv\Scripts\black src/
