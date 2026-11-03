# SPOOK Relay

Relay em tempo real do ecossistema SPOOK: chat, tarefas, notas, leitura de e-books, mídia, finanças e assistente de IA. Roda na Oracle Cloud (Always Free Micro) conectado via Tailscale.

## Arquitetura

```
┌─────────────┐     Socket.IO / HTTP      ┌──────────────────┐
│  Android    │ ◄────────────────────────► │  Oracle Relay    │
│  App        │     (Tailscale 100.70.88.101)│  (spook-relay)   │
└─────────────┘                             └────────┬─────────┘
                                                     │
                              ┌──────────────────────┘
                              ▼
                     ┌──────────────────┐     ┌──────────────────┐
                     │  Google Drive    │     │  Arch Server     │
                     │  (gdrive:SPOOK)  │     │  (pentestlink)   │
                     └──────────────────┘     └──────────────────┘
```

- **Android App** (PentestLink): chat, tasks, IA, reader, scans, pad, finance
- **Relay**: fonte da verdade para chat/tasks/media/relatorios/livros/pad/finance; IA (Groq) com tool-call para scans/comandos no Arch
- **Arch Server**: executa scans/comandos perigosos (bridge `ia_executar_*`), métricas, terminal, overlay GTK4
- **Google Drive**: backup persistente de tudo (rclone sync bidirecional)

## Requisitos

- Python 3.10+
- rclone configurado com remote `gdrive:` (Google Drive)
- Tailscale (para comunicação Arch ↔ Relay ↔ Android)
- `.env` com variáveis abaixo

## Configuração (`.env`)

```bash
# Chaves da IA (Groq)
GROQ_API_KEY=gsk_xxx
GROQ_MODEL=openai/gpt-oss-20b

# Token de pareamento dos aparelhos (obrigatório)
SPOOK_TOKEN=seu_token_aqui

# Opcional: sobrescreve caminhos padrão
SPOOK_DADOS=/home/ubuntu/spook-relay/data
SPOOK_MEDIA_DIR=/home/ubuntu/spook-relay/data/media
SPOOK_RELATORIOS_DIR=/home/ubuntu/spook-relay/data/relatorios
SPOOK_MEMORIA_DIR=/home/ubuntu/spook-relay/data/memoria_ia

# Opcional: CORS (origens permitidas para Socket.IO)
SPOOK_CORS_ORIGINS=http://127.0.0.1:5000,http://100.70.88.101:5000

# Opcional: usuário padrão da memória IA
SPOOK_USUARIO=taylor

# Opcional: extrai fatos a cada N perguntas do usuário
SPOOK_EXTRACAO_CADA=6
```

O token `SPOOK_TOKEN` deve ser **idêntico** ao usado no Arch (`~/.pentestlink/token_sessao.txt`) e no app Android (`Pareamento.obterToken()`).

## Como rodar

```bash
# 1. Clone e entre
git clone https://github.com/0v3rgod/spook-relay.git
cd spook-relay

# 2. Venv + deps
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Configure .env (copie .env.example e edite)
cp .env.example .env
# edite .env com suas chaves

# 4. Teste
python relay.py
# Deve subir em http://0.0.0.0:5000
# Health check: curl http://127.0.0.1:5000/healthz  →  "OK"
```

## Systemd (produção)

```ini
# /etc/systemd/system/spook-relay.service
[Unit]
Description=SPOOK Relay (Flask + Socket.IO)
After=network-online.target tailscale.service
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/spook-relay
Environment=PYTHONUNBUFFERED=1
Environment=SPOOK_REMOTE=gdrive:SPOOK
Environment=SPOOK_DADOS=/home/ubuntu/spook-relay/data
Environment=SPOOK_MEDIA_DIR=/home/ubuntu/spook-relay/data/media
Environment=SPOOK_RELATORIOS_DIR=/home/ubuntu/spook-relay/data/relatorios
Environment=SPOOK_REFRESHER=1
ExecStart=/home/ubuntu/spook-relay/.venv/bin/python relay.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now spook-relay.service
```

## Endpoints principais

| Método | Rota | Auth | Descrição |
|--------|------|------|-----------|
| GET | `/healthz` | não | Health check |
| GET | `/api/connected` | Bearer | Clientes Socket.IO conectados |
| Socket | `/` (default ns) | token no connect | chat, tasks, pad, notificações, IA tool-call |
| Socket | `/ia` | token no connect | `chat_ia_perguntar` / `chat_ia_resposta` |
| GET | `/livros` | Bearer | Lista prateleira (fichas por bookId) |
| POST | `/livros` | Bearer + multipart | Publica livro (arquivo + ficha) |
| GET | `/livros/<id>` | Bearer | Obtém ficha do livro |
| POST | `/livros/<id>` | Bearer + JSON | Atualiza posição/leitura (LWW) |
| GET | `/livros/<id>/arquivo` | Bearer | Download do arquivo (Range) |
| DELETE | `/livros/<id>` | Bearer | Tombstone (remove da lista, arquivo fica no Drive) |
| GET | `/media` | Bearer | Lista arquivos de mídia (bytes, mtime) |
| POST | `/upload` | Bearer + multipart | Upload de mídia/relatório |
| GET | `/media/<file>` | Bearer | Download de mídia |
| GET | `/relatorios` | Bearer | Lista relatórios (agrupa por base, mostra .docx) |
| GET | `/relatorios/<base>` | Bearer | Download do relatório (texto) |
| DELETE | `/relatorios` | Bearer + JSON | Apaga relatórios (`{"nomes": [...]}`) |
| GET | `/pad` | Bearer | Lista notas sincronizadas |
| POST | `/pad` | Bearer + JSON | Cria/atualiza nota (merge 3 vias) |
| GET | `/finance` | Bearer | Lista despesas/parcelas |
| GET | `/finance/calendar` | Bearer | Parcelas agrupadas por dia (mês/ano) |
| GET | `/ia/memoria` | Bearer | Ver/editar memória de longo prazo da IA |

## IA (Groq + tool-call)

O namespace `/ia` aceita `chat_ia_perguntar` com `{pergunta, sessao_id, usuario}`.  
Quando a IA pede ação, devolve **apenas** JSON:

```json
<system>
{"tool_call": {"acao": "executar_scan", "params": {"scan": "COMMON PORTS", "alvo": "192.168.0.1"}}}
</system>
```

Ações permitidas (allowlist no relay):
- `executar_scan` — params: `scan` (nome do catálogo), `alvo` (IP/CIDR/host)
- `executar_comando_notebook` — params: `comando` (nome do catálogo do Arch)
- `obter_info_sistema` — sem params

O relay encaminha via Socket.IO para o Arch (`ia_executar_scan|comando|info`), que executa localmente e devolve o resultado. O relay continua a conversa com a IA e emite a resposta final em `chat_ia_resposta`.

**Segurança**: comandos destrutivos (`shutdown`, `reboot`, `suspend`) têm `"confirmar": true` no catálogo e são **recusados** pela IA (o app pede confirmação na tela; a IA não consegue confirmar).

## Sincronismo com Google Drive

- `rclone sync --fast-list --update gdrive:SPOOK/... data/...` (bidirecional)
- Chat: um arquivo `.json` por mensagem em `gdrive:SPOOK/chat/`
- Tasks: um arquivo por task em `gdrive:SPOOK/tasks/<id>.json`
- Livros: ficha em `gdrive:SPOOK/reading/livros/<bookId>.json`, arquivo em `gdrive:SPOOK/reading/arquivos/<bookId>.<ext>`
- Relatórios: `gdrive:SPOOK/relatorios/<base>.docx` (editável no Google Docs)
- Pad: `gdrive:SPOOK/pad/<id>.json` + histórico
- Finance: `gdrive:SPOOK/finance/expenses.json`

O guard timer (`spook-relay-guard.timer`, a cada 5 min) restaura `relay.py.good` se endpoints críticos sumirem.

## Desenvolvimento

```bash
# Testes rápidos
TOKEN=$(cat ~/.pentestlink/token_sessao.txt)
for r in /livros /pad /media /relatorios /finance /api/connected /ia/memoria; do
  curl -s -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:5000$r" -o /dev/null -w "%{http_code} $r\n"
done
```

## Licença

MIT — uso pessoal/estudo. O código é público para portfólio; não use em produção sem revisão de segurança.
