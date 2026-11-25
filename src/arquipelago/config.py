"""Constantes de tempo e de rede usadas por todos os módulos."""

# Porta TCP padrão do servidor gRPC e porta UDP da descoberta automática na LAN.
DEFAULT_GRPC_PORT = 50051
DISCOVERY_PORT = 50050
DISCOVERY_INTERVAL = 1.0
DEFAULT_CLUSTER = "arquipelago"

# Detecção de falhas (segundos).
HEARTBEAT_INTERVAL = 0.5     # líder -> membros
HEARTBEAT_RPC_TIMEOUT = 1.0
MEMBER_FAILURE_TIMEOUT = 3.0  # líder declara um membro morto
LEADER_FAILURE_TIMEOUT = 3.0  # seguidor suspeita do líder e inicia eleição

# Eleição (Bully).
JOIN_GRACE = 2.5              # tempo sem líder conhecido antes de convocar eleição
ELECTION_REPLY_TIMEOUT = 1.0  # espera por "OK" de nós com id maior
COORDINATOR_WAIT = 4.0        # espera pela mensagem COORDINATOR após receber "OK"

# Chamadas gRPC em geral.
RPC_TIMEOUT = 2.0
RPC_RETRIES = 2
MAX_MESSAGE_BYTES = 64 * 1024 * 1024

# Replicação.
REPLICATION_INTERVAL = 0.2
LOG_LINES_KEPT = 400

# Berkeley.
BERKELEY_PERIOD = 30.0
BERKELEY_OUTLIER_TOLERANCE = 10.0  # s: leituras mais distantes da mediana são descartadas
BERKELEY_SYNCED_TOLERANCE = 0.1    # s: idem, depois que o cluster já foi sincronizado uma vez
BERKELEY_MAX_RTT = 0.5             # s: leituras com RTT maior são descartadas

# Saída do processo quando a falha é simulada (o supervisor reinicia o nó).
CRASH_EXIT_CODE = 3

GRPC_CHANNEL_OPTIONS = [
    ("grpc.max_send_message_length", MAX_MESSAGE_BYTES),
    ("grpc.max_receive_message_length", MAX_MESSAGE_BYTES),
    # Reconexão rápida: um nó reiniciado volta a ser alcançável em ~1 s.
    ("grpc.initial_reconnect_backoff_ms", 300),
    ("grpc.min_reconnect_backoff_ms", 300),
    ("grpc.max_reconnect_backoff_ms", 1500),
    ("grpc.enable_retries", 0),
]
