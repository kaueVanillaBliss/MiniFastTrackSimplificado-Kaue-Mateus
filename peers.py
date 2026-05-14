import socket
import threading
import os
import json
import hashlib

class Peer:
    def __init__(self, host, port, shared_dir):
        self.host = host
        self.port = port
        # O ID do peer será o seu IP:Porta
        self.id = f"{host}:{port}" 
        self.shared_dir = shared_dir
        self.peer_list = {} # Dicionário para guardar quem está online: {id: [ip, porta]}
        self.is_discovery = False

        # Cria a pasta de compartilhamento na máquina local se ela não existir
        if not os.path.exists(self.shared_dir):
            os.makedirs(self.shared_dir)

        # Configura o socket TCP (Servidor embutido)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))

    def start_listening(self):
        """Fica escutando novas conexões em background"""
        self.server_socket.listen()
        print(f"\n[+] Peer inciado! ID: {self.id}")
        print(f"[+] Escutando na porta {self.port}...")

        while True:
            conn, addr = self.server_socket.accept()
            # Para cada nova conexão, abrimos uma thread para não travar o peer principal
            thread = threading.Thread(target=self.handle_client, args=(conn, addr))
            thread.start()

    def enviar_mensagem(self, socket_conn, mensagem):
        """Converte um dicionário para JSON e envia pelo socket"""
        dados = json.dumps(mensagem).encode('utf-8')
        socket_conn.send(dados)

    def receber_mensagem(self, socket_conn):
        """Recebe dados do socket e converte de volta para dicionário"""
        try:
            dados = socket_conn.recv(4096).decode('utf-8')
            if dados:
                return json.loads(dados)
        except:
            return None
        return None

    def handle_client(self, conn, addr):
        """Lida com as requisições que chegam de outros peers"""
        mensagem = self.receber_mensagem(conn)
        
        if mensagem:
            acao = mensagem.get("acao")

            # Ação 1: Alguém quer entrar na rede
            if acao == "REGISTRAR":
                novo_id = mensagem.get("id")
                ip = mensagem.get("ip")
                porta = mensagem.get("porta")
                
                self.peer_list[novo_id] = [ip, porta]
                print(f"\n[+] Novo peer registrado: {novo_id}")
                
                resposta = {
                    "acao": "LISTA_ATUALIZADA",
                    "peers": self.peer_list
                }
                self.enviar_mensagem(conn, resposta)

            # Ação 2: Alguém quer ver meus arquivos
            elif acao == "LISTAR_ARQUIVOS":
                arquivos = self.get_local_files()
                resposta = {
                    "acao": "RESPOSTA_ARQUIVOS",
                    "arquivos": arquivos
                }
                self.enviar_mensagem(conn, resposta)

            # Ação 3: Alguém quer fazer download de um arquivo meu
            elif acao == "BAIXAR_ARQUIVO":
                nome_arquivo = mensagem.get("nome_arquivo")
                caminho_arquivo = os.path.join(self.shared_dir, nome_arquivo)
                
                if os.path.exists(caminho_arquivo):
                    # Calcula o Hash SHA-256 do arquivo para garantir a integridade
                    sha256_hash = hashlib.sha256()
                    with open(caminho_arquivo, "rb") as f:
                        for byte_block in iter(lambda: f.read(4096), b""):
                            sha256_hash.update(byte_block)
                    hash_arquivo = sha256_hash.hexdigest()
                    tamanho = os.path.getsize(caminho_arquivo)
                    
                    # Envia metadados do arquivo primeiro
                    resposta = {
                        "acao": "INICIAR_DOWNLOAD",
                        "tamanho": tamanho,
                        "hash": hash_arquivo
                    }
                    self.enviar_mensagem(conn, resposta)
                    
                    # Aguarda confirmação do cliente antes de mandar os bytes
                    confirmacao = self.receber_mensagem(conn)
                    if confirmacao and confirmacao.get("acao") == "PRONTO_PARA_RECEBER":
                        with open(caminho_arquivo, "rb") as f:
                            while (chunk := f.read(4096)):
                                conn.send(chunk)
                        print(f"\n[+] Arquivo '{nome_arquivo}' enviado para {addr}.")
                else:
                    self.enviar_mensagem(conn, {"acao": "ERRO", "mensagem": "Arquivo inexistente no servidor"})

        conn.close()

    def get_local_files(self):
        """Lista os arquivos da pasta compartilhada"""
        return os.listdir(self.shared_dir)

    def solicitar_arquivos(self, id_alvo):
        """Pede a lista de arquivos para um peer específico"""
        if id_alvo not in self.peer_list:
            print(f"[!] Peer {id_alvo} não encontrado na sua lista.")
            return

        ip_alvo, porta_alvo = self.peer_list[id_alvo]
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((ip_alvo, int(porta_alvo)))
            
            self.enviar_mensagem(sock, {"acao": "LISTAR_ARQUIVOS"})
            
            resposta = self.receber_mensagem(sock)
            if resposta and resposta.get("acao") == "RESPOSTA_ARQUIVOS":
                arquivos = resposta.get("arquivos", [])
                print(f"\n--- Arquivos no Peer {id_alvo} ---")
                if arquivos:
                    for arq in arquivos:
                        print(f" - {arq}")
                else:
                    print(" Nenhum arquivo compartilhado no momento.")
                print("-----------------------------------")
            
            sock.close()
        except (ConnectionRefusedError, socket.timeout):
            print(f"[!] Erro: Não foi possível conectar ao peer {id_alvo}. Ele pode ter caído.")
            if id_alvo in self.peer_list:
                del self.peer_list[id_alvo]

    def baixar_arquivo(self, id_alvo, nome_arquivo):
        """Realiza o download P2P de um arquivo e verifica a integridade via Hash"""
        if id_alvo not in self.peer_list:
            print(f"[!] Peer {id_alvo} não encontrado.")
            return

        ip_alvo, porta_alvo = self.peer_list[id_alvo]
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip_alvo, int(porta_alvo)))
            
            self.enviar_mensagem(sock, {"acao": "BAIXAR_ARQUIVO", "nome_arquivo": nome_arquivo})
            
            resposta = self.receber_mensagem(sock)
            
            if resposta and resposta.get("acao") == "INICIAR_DOWNLOAD":
                tamanho_esperado = resposta.get("tamanho")
                hash_esperado = resposta.get("hash")
                
                self.enviar_mensagem(sock, {"acao": "PRONTO_PARA_RECEBER"})
                
                caminho_salvar = os.path.join(self.shared_dir, nome_arquivo)
                bytes_recebidos = 0
                sha256_hash = hashlib.sha256()
                
                print(f"[*] Iniciando download de '{nome_arquivo}' ({tamanho_esperado} bytes)...")
                
                with open(caminho_salvar, "wb") as f:
                    while bytes_recebidos < tamanho_esperado:
                        chunk = sock.recv(min(4096, tamanho_esperado - bytes_recebidos))
                        if not chunk:
                            break
                        f.write(chunk)
                        sha256_hash.update(chunk)
                        bytes_recebidos += len(chunk)
                
                # Verificação de integridade
                hash_calculado = sha256_hash.hexdigest()
                if hash_calculado == hash_esperado:
                    print(f"[+] Download concluído com sucesso! (Integridade validada: {hash_calculado[:8]}...)")
                else:
                    print(f"[!] ALERTA: Arquivo corrompido durante a transferência! O hash não bate.")
                    print("[!] Apagando arquivo defeituoso por segurança...")
                    os.remove(caminho_salvar)
            
            elif resposta and resposta.get("acao") == "ERRO":
                print(f"[!] O peer relatou um erro: {resposta.get('mensagem')}")
                
            sock.close()
        except Exception as e:
            print(f"[!] Erro crítico durante o download: {e}")

    def entrar_na_rede(self, discovery_host, discovery_port):
        """Tenta encontrar o Ponto de Descoberta. Se não achar, assume o papel."""
        if self.host == discovery_host and self.port == discovery_port:
            print("[*] Iniciando como Ponto de Descoberta primário.")
            self.is_discovery = True
            return

        print(f"[*] Buscando Ponto de Descoberta em {discovery_host}:{discovery_port}...")
        
        try:
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            temp_sock.connect((discovery_host, discovery_port))
            
            msg_registro = {
                "acao": "REGISTRAR",
                "id": self.id,
                "ip": self.host,
                "porta": self.port
            }
            self.enviar_mensagem(temp_sock, msg_registro)
            
            resposta = self.receber_mensagem(temp_sock)
            if resposta and resposta.get("acao") == "LISTA_ATUALIZADA":
                self.peer_list = resposta.get("peers", {})
                print(f"[+] Conectado à rede com sucesso!")
            
            temp_sock.close()

        except ConnectionRefusedError:
            print("[!] Ponto de Descoberta não encontrado ou offline.")
            print("[*] Assumindo o papel de Ponto de Descoberta (Líder) da rede.")
            self.is_discovery = True


# ==========================================
# BLOCO PRINCIPAL (EXECUÇÃO E MENU)
# ==========================================
if __name__ == "__main__":
    # Pede a porta para permitir testes locais com vários peers no mesmo PC
    porta_input = input("Digite a porta para iniciar este peer (ex: 5000, 5001): ")
    PORTA = int(porta_input)
    IP_LOCAL = '127.0.0.1'
    PASTA_LOCAL = f'./arquivos_peer_{PORTA}'

    # O IP e Porta base que todo mundo tenta acessar primeiro para se descobrir
    IP_DESCOBERTA = '127.0.0.1'
    PORTA_DESCOBERTA = 5000

    meu_peer = Peer(IP_LOCAL, PORTA, PASTA_LOCAL)

    # Inicia o servidor em background
    listener_thread = threading.Thread(target=meu_peer.start_listening)
    listener_thread.daemon = True
    listener_thread.start()

    # Tenta se registrar na rede
    meu_peer.entrar_na_rede(IP_DESCOBERTA, PORTA_DESCOBERTA)

    # Interface interativa do terminal
    try:
        while True:
            print("\n" + "="*35)
            print(" SISTEMA P2P - MENU PRINCIPAL ".center(35, "="))
            print("1. Listar Peers conhecidos na rede")
            print("2. Ver arquivos de um Peer específico")
            print("3. Fazer download de um arquivo")
            print("4. Sair")
            print("="*35)
            cmd = input("Escolha uma opção: ")

            if cmd == '1':
                print("\n--- Peers Ativos ---")
                for pid in meu_peer.peer_list:
                    print(f" -> ID: {pid}")
                if not meu_peer.peer_list:
                    print(" Nenhum outro peer na rede no momento.")
            
            elif cmd == '2':
                alvo = input("Digite o ID do Peer alvo (ex: 127.0.0.1:5000): ")
                meu_peer.solicitar_arquivos(alvo)
            
            elif cmd == '3':
                alvo = input("Digite o ID do Peer que possui o arquivo: ")
                arquivo = input("Digite o NOME EXATO do arquivo (com extensão, ex: foto.png): ")
                meu_peer.baixar_arquivo(alvo, arquivo)

            elif cmd == '4':
                print("Desconectando e encerrando o nó...")
                break
            else:
                print("Opção inválida. Tente novamente.")
    except KeyboardInterrupt:
        print("\nEncerrando forçadamente pelo usuário...")