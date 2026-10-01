# Sistema de Compartilhamento de Arquivos P2P (Peer-to-Peer) [Trabalho da Disciplina de Sistemas Distribuidos]

Este projeto implementa um sistema de compartilhamento de arquivos descentralizado (P2P) totalmente desenvolvido em Python, utilizando sockets TCP e a biblioteca standard. O sistema permite que múltiplos nós (peers) se conectem entre si, descubram a rede automaticamente e transfiram arquivos com verificação rigorosa de integridade.

## Funcionalidades Principais

* **Descoberta Automática na Rede (Auto-Discovery):** Ao ser iniciado, um peer varre um intervalo de portas (5000 a 5010) para procurar uma rede ativa. Se não encontrar, assume automaticamente o papel de "Nó Líder" (Ponto de Descoberta).
* **Comunicação Assíncrona:** Implementação multithread que permite ao sistema escutar novas conexões em segundo plano sem bloquear o menu interativo do usuário.
* **Listagem Remota:** Capacidade de solicitar e visualizar a lista de arquivos disponíveis em qualquer nó da rede.
* **Transferência Segura de Arquivos:** Utiliza o algoritmo SHA-256 para calcular a hash do arquivo antes do envio e validá-la após o download. Arquivos corrompidos são automaticamente detectados e apagados.
* **Criação Automática de Diretórios:** Cada peer gera a sua própria pasta local de compartilhamento (ex: arquivos_peer_5000) para isolar e organizar o conteúdo.

## Pré-requisitos

* **Python 3.6+** instalado no sistema.
* Não é necessária a instalação de nenhuma biblioteca externa (o código utiliza apenas as bibliotecas integradas: socket, threading, os, json e hashlib).

## Como Utilizar

### Passo 1: Iniciar os Peers
Abra dois ou mais terminais no seu computador para simular diferentes nós na rede.
Em cada terminal, execute o script:

```bash
python nome_do_arquivo.py
