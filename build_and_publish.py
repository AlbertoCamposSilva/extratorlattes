import re
import subprocess
import sys
import shutil
from pathlib import Path

def update_version(file_path):
    """Lê o pyproject.toml, incrementa a versão e salva."""
    content = file_path.read_text(encoding="utf-8")
    
    # Busca o padrão version = "x.y.z"
    match = re.search(r'version\s*=\s*"(\d+)\.(\d+)\.(\d+)"', content)
    if not match:
        print("Erro: Versão não encontrada no pyproject.toml")
        sys.exit(1)
        
    major, minor, patch = map(int, match.groups())
    new_version = f"{major}.{minor}.{patch + 1}"
    
    # Substitui no conteúdo
    new_content = content.replace(f'version = "{major}.{minor}.{patch}"', f'version = "{new_version}"')
    file_path.write_text(new_content, encoding="utf-8")
    
    return new_version

def run_command(command):
    """Executa comandos de shell com interrupção em caso de erro."""
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar: {command}")
        sys.exit(1)

def main():
    pyproject_path = Path("pyproject.toml")
    
    if not pyproject_path.exists():
        print("Erro: pyproject.toml não encontrado na pasta atual.")
        return

    # 1. Incrementar versão
    new_ver = update_version(pyproject_path)
    print(f"--- Versão atualizada para {new_ver} ---")

    # 2. Limpar pasta dist anterior
    if Path("dist").exists():
        shutil.rmtree("dist")

    # 3. Git: Add e Commit
    print("--- Realizando commit e tag no Git ---")
    run_command("git add .")
    run_command(f'git commit -m "Release v{new_ver}"')
    run_command(f'git tag -a v{new_ver} -m "Version {new_ver}"')
    run_command("git push origin main --tags")

    # 4. Build do pacote
    print("--- Gerando arquivos de distribuição (Build) ---")
    run_command("python -m build")

    # 5. Upload para o PyPI
    # Nota: Requer que o Token esteja configurado no arquivo .pypirc ou como variável de ambiente
    print("--- Fazendo upload para o PyPI via Twine ---")
    run_command("python -m twine upload dist/*")

    print(f"\nSucesso! Versão {new_ver} publicada no GitHub e PyPI.")

if __name__ == "__main__":
    main()