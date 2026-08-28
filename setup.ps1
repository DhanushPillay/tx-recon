Write-Host "Creating Python virtual environment..."
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt

Write-Host "Starting Docker containers..."
docker-compose up -d

Write-Host "Setup complete! You can now run the Python scripts."
