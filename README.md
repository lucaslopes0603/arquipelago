# Arquipélago — Algoritmo Genético Distribuído (modelo de ilhas) para o TSP

Trabalho prático de Computação Distribuída (PUC Minas, 2026/2).

O sistema resolve o **Problema do Caixeiro Viajante (TSP)** com um **algoritmo genético
distribuído no modelo de ilhas**: cada nó evolui uma subpopulação (uma "ilha") e, a cada
época, envia seus melhores indivíduos para a próxima ilha de um anel lógico (migração).
Um nó líder coordena o trabalho; se ele falhar, outro é eleito e continua de onde parou.

O relatório da entrega 2 está em
[entrega2final/Relatorio_Entrega2_Arquipelago.pdf](entrega2final/Relatorio_Entrega2_Arquipelago.pdf).
A mesma pasta tem o código compactado e o executável pronto em `entrega2final/Demonstracao/`.

## Requisitos implementados

| Requisito do enunciado | Onde está |
|---|---|
| RPC (gRPC) | [src/arquipelago/rpc/arquipelago.proto](src/arquipelago/rpc/arquipelago.proto), [servicer.py](src/arquipelago/servicer.py) |
| Sincronização de relógios físicos (Berkeley) | [berkeley.py](src/arquipelago/berkeley.py), [clocks.py](src/arquipelago/clocks.py) |
| Sincronização de relógios lógicos (Lamport) | [clocks.py](src/arquipelago/clocks.py), [events.py](src/arquipelago/events.py) |
| Eleição de líder (Bully / Valentão) | [election.py](src/arquipelago/election.py) |
| Tratamento de falhas (heartbeat, timeout, retry, checkpoint, reatribuição) | [membership.py](src/arquipelago/membership.py), [jobs.py](src/arquipelago/jobs.py), [island.py](src/arquipelago/island.py) |
| Controle de réplicas (primário-backup) | [replication.py](src/arquipelago/replication.py), [store.py](src/arquipelago/store.py) |
| Análise de desempenho vs. uma máquina | [scripts/benchmark.py](scripts/benchmark.py), fase 4 da bateria de testes |

Entrega 1 pede no mínimo 2 desses itens; os requisitos escolhidos foram eleição de líder (Bully) e relógios lógicos (Lamport). O gRPC é a base da comunicação e o Berkeley foi um extra.
Entrega 2 pede tratamento de falhas e no mínimo 1 item extra; foram implementados controle de réplicas e análise de desempenho.

## Estrutura

```
src/arquipelago/
  rpc/arquipelago.proto   contrato gRPC (mensagens e serviço)
  node.py                 o nó: papel, membros, chamadas RPC, logs
  membership.py           entrada no cluster, heartbeats, detecção de falhas
  election.py             eleição Bully (com mandatos)
  replication.py          replicação primário-backup do estado do coordenador
  store.py                armazenamento chave-valor versionado (deltas/snapshots)
  berkeley.py, clocks.py  Berkeley e Lamport
  events.py               log de eventos e verificação causal (Lamport)
  jobs.py                 criação de jobs, distribuição e reatribuição de ilhas
  island.py               execução de uma ilha + migração
  ga.py, tsp.py           algoritmo genético e problema (sem rede)
  orchestrator.py         bateria de testes automática (roda no líder)
  report.py               relatório do teste
  discovery.py            descoberta automática na rede (broadcast UDP)
  launcher.py             executável: supervisor + tela
  cli.py                  linha de comando
tests/                    testes automatizados (pytest)
scripts/benchmark.py      análise de desempenho
scripts/graficos.py       gráficos da análise de desempenho
empacotamento/            geração do Arquipelago.exe e arquivos de apoio (.bat, peers_exemplo.txt, LEIA-ME.txt)
demonstrar.command        demonstração no macOS/Linux
entrega2final/            relatório em PDF, código compactado e executável da entrega 2
```

## Como executar

### 1. Executável (laboratório, sem instalar nada)

1. Copie o `Arquipelago.exe` para **cada** computador. Ele já vem pronto na pasta
   `entrega2final/Demonstracao/`. Para gerar de novo a partir do código, veja a seção 4 (o executável fica em `dist\`).
2. Dê dois cliques nele em cada um. Se o Windows perguntar sobre o firewall, clique em **Permitir**.
3. Os computadores se encontram sozinhos (broadcast UDP). Pressione **ENTER** em qualquer um deles,
   ou espere 40 s sem novos computadores entrando.
4. A bateria de testes leva cerca de 2 a 3 minutos. Durante ela, um computador e depois o líder
   caem de propósito e reiniciam sozinhos. No fim, todas as janelas mostram o relatório, que também
   é salvo em `arquipelago_resultados/`.

Se a rede bloquear broadcast, digite o IP de outro computador na janela e tecle ENTER, ou use um
`peers.txt` (veja `peers_exemplo.txt`). Para liberar o firewall de uma vez, rode
`liberar_firewall.bat` como administrador.

Sem outros computadores, `simular_3_nos_neste_pc.bat` roda a mesma bateria com 3 nós locais.

Esses arquivos de apoio ficam ao lado do executável (em `entrega2final/Demonstracao/`, ou em `dist\` depois de
gerar o executável) e, no código-fonte, em `empacotamento/`. Os `.bat` precisam estar na mesma
pasta do `Arquipelago.exe`.

### 2. A partir do código (Python 3.11+)

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
$env:PYTHONPATH = "src"
```

Nós manuais (um terminal por nó; em máquinas diferentes, troque 127.0.0.1 pelos IPs):

```powershell
python -m arquipelago node --id 1 --port 50051 --peers 127.0.0.1:50052,127.0.0.1:50053
python -m arquipelago node --id 2 --port 50052 --peers 127.0.0.1:50051,127.0.0.1:50053
python -m arquipelago node --id 3 --port 50053 --peers 127.0.0.1:50051,127.0.0.1:50052
```

Em outro terminal:

```powershell
python -m arquipelago status --addr 127.0.0.1:50051           # papel, líder, membros, log
python -m arquipelago submit --addr 127.0.0.1:50051 --wait    # job do AG (qualquer nó encaminha ao líder)
python -m arquipelago test   --addr 127.0.0.1:50051           # bateria de testes completa
```

Para provocar falhas manualmente, feche (Ctrl+C) o terminal de um nó durante um job e observe
os outros detectarem a falha. Fechar o do líder dispara uma nova eleição.

Outros modos:

```powershell
python -m arquipelago local --nodes 4            # simula 4 nós nesta máquina + bateria de testes
python -m arquipelago lan                        # o mesmo que o executável (rede local)
```

### 3. Docker (4 "computadores" com IPs diferentes)

```powershell
docker compose up --build
```

Os resultados ficam em `resultados_docker/noX/arquipelago_resultados/`.

### 4. Testes, desempenho e executável

```powershell
pip install -r requirements-dev.txt
python -m pytest tests                                   # 17 testes (unidades + cluster gRPC real)
python scripts/benchmark.py --reps 2                     # k = 1, 2, 3, 4, 6 e 8 nós (Tabela 3) -> relatorio/dados/ (criada na hora)
python scripts/graficos.py                               # gráficos -> relatorio/figuras/
powershell -ExecutionPolicy Bypass -File empacotamento\gerar_exe.ps1   # gera dist\Arquipelago.exe
```

Se alterar o `.proto`, regenere o código:

```powershell
python -m grpc_tools.protoc -I src --python_out=src --grpc_python_out=src src/arquipelago/rpc/arquipelago.proto
```

## Portas

| Porta | Protocolo | Uso |
|---|---|---|
| 50051 em diante | TCP | servidor gRPC de cada nó |
| 50050 | UDP | descoberta automática (broadcast) |

## Demonstração no macOS / Linux

Requer Python 3.11 ou mais recente. Na pasta do projeto:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
./demonstrar.command
```

No macOS, também é possível dar dois cliques em `demonstrar.command` após a instalação.
O modo local inicia três processos, executa a bateria completa e encerra os nós ao terminar.
As quedas do trabalhador e do líder são provocadas pela própria demonstração.
Relatórios ficam em `demonstracao_execucao/arquipelago_resultados/`.
Para repetir, execute o mesmo comando; os relatórios têm identificação por data e hora.

Para executar os testes:

```bash
pip install pytest
python -m pytest tests -q
```
