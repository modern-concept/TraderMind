# Criação da estrutura de diretórios
$base = "TraderMind"
$dirs = @(
    "$base\docker\spark",
    "$base\scripts",
    "$base\data"
)

foreach ($dir in $dirs) {
    New-Item -Path $dir -ItemType Directory -Force | Out-Null
}

# Criação de arquivos vazios
New-Item -Path "$base\docker\spark\Dockerfile" -ItemType File -Force | Out-Null
New-Item -Path "$base\scripts\process_data.py" -ItemType File -Force | Out-Null
New-Item -Path "$base\docker-compose.yml" -ItemType File -Force | Out-Null

Write-Host "✅ Estrutura de projeto 'TraderMind' criada com sucesso!"
