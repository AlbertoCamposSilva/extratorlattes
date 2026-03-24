import re
import subprocess
import sys
import os
import urllib.request
import urllib.error
import json
import shutil
import zipfile
from pathlib import Path
from dotenv import load_dotenv

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

def run_command(command, error_message=None):
    """Executa comandos de shell com interrupção em caso de erro."""
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar: {command}")
        if error_message:
            print(error_message)
        sys.exit(1)

def get_github_repo():
    """Extrai o 'owner' e o 'repo' a partir da configuração do git origin."""
    try:
        out = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()
        # Extrai de: https://github.com/OWNER/REPO.git ou git@github.com:OWNER/REPO.git
        match = re.search(r'github\.com[:/](.+?)/(.+?)(?:\.git)?$', out)
        if match:
            return match.group(1), match.group(2)
    except Exception:
        pass
    return None, None

def create_github_release(version):
    """Cria uma nova release no GitHub usando a REST API."""
    try:
        load_dotenv(r"E:\Python\.env")
    except ImportError:
        print("--- Aviso: python-dotenv não instalado. Lendo variáveis nativas do sistema. ---")

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        print("--- Aviso: Variável de ambiente 'GITHUB_TOKEN' não encontrada. Pulando criação de Release no GitHub. ---")
        print("Dica: Defina o GITHUB_TOKEN no Windows para automatizar as releases.")
        return

    owner, repo = get_github_repo()
    if not owner or not repo:
        print("--- Aviso: Não foi possível determinar o repositório (origin) no Git. Pulando Release no GitHub. ---")
        return

    print(f"--- Criando Release v{version} via API do GitHub em {owner}/{repo} ---")
    url = f"https://api.github.com/repos/{owner}/{repo}/releases"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    data = {"tag_name": f"v{version}", "name": f"Release v{version}", "body": f"Versão automatizada {version}.", "draft": False, "prerelease": False}
    req = urllib.request.Request(url, data=json.dumps(data).encode("utf-8"), headers=headers, method="POST")
    
    try:
        with urllib.request.urlopen(req) as response:
            res_data = json.loads(response.read().decode())
            print(f"Sucesso! Release publicada no GitHub: {res_data.get('html_url')}")
    except urllib.error.HTTPError as e:
        print(f"Erro na API do GitHub ({e.code}): {e.read().decode()}")
    except Exception as e:
        print(f"Erro desconhecido ao chamar a API do GitHub: {e}")

def main():
    pyproject_path = Path("pyproject.toml")
    
    if not pyproject_path.exists():
        print("Erro: pyproject.toml não encontrado na pasta atual.")
        return

    # 0. Instalar dependências de build
    print("--- Instalando dependências de build ---")
    run_command(f'"{sys.executable}" -m pip install --upgrade build twine python-dotenv')

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
    create_github_release(new_ver)

    # 4. Build do pacote
    print("--- Gerando arquivos de distribuição (Build) ---")
    run_command(f'"{sys.executable}" -m build')

    # Verificação de integridade do pacote
    print("--- Verificando integridade do pacote gerado ---")
    whl_files = list(Path("dist").glob("*.whl"))
    if not whl_files:
        print("Erro: Nenhum arquivo .whl gerado.")
        sys.exit(1)
    
    # Verifica se existe conteúdo do pacote (pasta extratorlattes) dentro do wheel
    package_name = "extratorlattes"
    with zipfile.ZipFile(whl_files[0], 'r') as z:
        has_content = any(f.startswith(f"{package_name}/") or f.startswith(f"{package_name}.py") for f in z.namelist())
    
    if not has_content:
        print(f"\n[ERRO CRÍTICO] O pacote gerado parece vazio! A pasta '{package_name}' não foi encontrada no arquivo .whl.")
        print(f"Solução: Certifique-se de que existe um arquivo '__init__.py' dentro da pasta '{package_name}'.")
        sys.exit(1)

    # 5. Upload para o PyPI
    # Nota: Requer que o Token esteja configurado no arquivo .pypirc ou como variável de ambiente
    print("--- Fazendo upload para o PyPI via Twine ---")
    
    # Verificação prévia do formato do token no .pypirc
    pypirc_path = Path.home() / ".pypirc"
    if pypirc_path.exists():
        try:
            content = pypirc_path.read_text(encoding="utf-8", errors="ignore")
            if "password" in content and "pypi-" not in content:
                print(f"\n[ALERTA] O token em {pypirc_path} parece incorreto (não começa com 'pypi-').")
                print("Certifique-se de copiar o token completo gerado no site (começando com 'pypi-') e não o ID.\n")
        except Exception:
            pass
    
    auth_help = (
        "\n[DICA] Falha no upload. Verifique sua autenticação.\n"
        "Crie um arquivo .pypirc na sua pasta de usuário (%USERPROFILE%\\.pypirc) com o conteúdo:\n"
        "[pypi]\n"
        "  username = __token__\n"
        "  password = pypi-seu-token-aqui\n"
    )
    run_command(f'"{sys.executable}" -m twine upload --verbose dist/*', error_message=auth_help)

    print(f"\nSucesso! Versão {new_ver} publicada no GitHub e PyPI.")

if __name__ == "__main__":
    main()