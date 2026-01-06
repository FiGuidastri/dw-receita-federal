import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
from tqdm import tqdm

def baixar_arquivos_receita(url_base, pasta_destino='dados_cnpj'):
    """
    Baixa todos os arquivos da página de dados abertos da Receita Federal
    """
    # Criar pasta de destino se não existir
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
    
    print(f"Buscando lista de arquivos em: {url_base}")
    
    # Fazer requisição para obter a página
    response = requests.get(url_base)
    response.raise_for_status()
    
    # Parse do HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Encontrar todos os links de arquivos
    links = soup.find_all('a')
    arquivos = []
    
    for link in links:
        href = link.get('href')
        if href and not href.startswith('?') and href != '../':
            arquivos.append(href)
    
    print(f"Encontrados {len(arquivos)} arquivos para download\n")
    
    # Baixar cada arquivo
    for arquivo in arquivos:
        url_arquivo = urljoin(url_base, arquivo)
        caminho_local = os.path.join(pasta_destino, arquivo)
        
        # Verificar se arquivo já existe
        if os.path.exists(caminho_local):
            print(f"Arquivo já existe, pulando: {arquivo}")
            continue
        
        print(f"Baixando: {arquivo}")
        
        try:
            # Download com barra de progresso
            response = requests.get(url_arquivo, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(caminho_local, 'wb') as f:
                if total_size == 0:
                    f.write(response.content)
                else:
                    with tqdm(total=total_size, unit='B', unit_scale=True, desc=arquivo) as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                            pbar.update(len(chunk))
            
            print(f"✓ Concluído: {arquivo}\n")
            
        except Exception as e:
            print(f"✗ Erro ao baixar {arquivo}: {str(e)}\n")
    
    print("Download concluído!")

if __name__ == "__main__":
    url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-11/"
    baixar_arquivos_receita(url)