"""Ponto de entrada do executável Arquipelago.exe.

Duplo clique (sem argumentos) = modo "lan": teste distribuído na rede local.
O próprio executável é reutilizado como processo do nó ("Arquipelago.exe node ...").
"""

import sys
import traceback

from arquipelago.cli import main

if __name__ == "__main__":
    argv = sys.argv[1:] or ["lan"]
    interactive = argv[0] in ("lan", "local")
    try:
        main(argv)
    except SystemExit:
        raise
    except BaseException:
        traceback.print_exc()
        if interactive:
            try:
                input("\nOcorreu um erro inesperado (detalhes acima). Pressione ENTER para fechar.")
            except EOFError:
                pass
        sys.exit(1)
