"""Gera o relatório em texto da bateria de testes (exibido em todas as telas e salvo em arquivo)."""

import textwrap
from datetime import datetime

from .tsp import is_valid_tour


def _fmt_t(ts) -> str:
    return datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M:%S") if ts else "?"


def _s(v, nd=1, suffix="s") -> str:
    return "?" if v is None else f"{v:.{nd}f}{suffix}"


def build_report(test: dict, members) -> tuple[str, list[dict]]:
    r = test.get("results", {})
    checks: list[dict] = []
    lines: list[str] = []
    w = lines.append

    def check(name, status, detail):
        checks.append({"name": name, "status": status, "detail": detail})

    def err(phase):
        res = r.get(phase) or {}
        return res.get("error")

    # ---------------------------------------------------------------- verificações
    con = r.get("connectivity") or {}
    nodes = con.get("nodes", [])
    if err("connectivity"):
        check("Comunicação gRPC entre os computadores", "FALHOU", err("connectivity"))
    elif not nodes:
        check("Comunicação gRPC entre os computadores", "PULADO", "apenas 1 nó no cluster")
    else:
        bad = [n for n in nodes if n["received"] == 0]
        avg = [n["rtt_avg_ms"] for n in nodes if n["rtt_avg_ms"] is not None]
        check("Comunicação gRPC entre os computadores", "OK" if not bad else "FALHOU",
              f"{len(nodes) - len(bad)}/{len(nodes)} nós responderam; RTT médio "
              f"{sum(avg) / len(avg):.2f} ms" if avg else "nenhuma resposta")

    leader = con.get("leader") or {}
    check("Eleição de líder (Bully)", "OK" if leader else "FALHOU",
          f"líder nó {leader.get('id')} ({leader.get('host')}), mandato {con.get('term')}" if leader else "sem líder")

    bk = r.get("berkeley") or {}
    if err("berkeley"):
        check("Sincronização de relógios físicos (Berkeley)", "FALHOU", err("berkeley"))
    elif bk:
        before, after = bk.get("spread_before_ms", 0), bk.get("spread_after_ms", 0)
        ok = after <= 20 or after <= before / 10
        check("Sincronização de relógios físicos (Berkeley)", "OK" if ok else "FALHOU",
              f"dispersão {before:.1f} ms -> {after:.2f} ms")

    dj = r.get("dist_job") or {}
    if err("dist_job") or not dj.get("best_tour"):
        check("AG distribuído em ilhas", "FALHOU", err("dist_job") or "sem resultado")
    else:
        valid = is_valid_tour(dj["best_tour"], len(dj["best_tour"]))
        check("AG distribuído em ilhas", "OK" if valid else "FALHOU",
              f"{dj['n_islands']} ilhas, melhor rota {dj['best_length']:.1f} em {dj['wall_seconds']:.1f}s"
              + ("" if valid else " (rota inválida!)"))

    bl = r.get("baseline_job") or {}
    if bl.get("skipped"):
        check("Desempenho vs. execução em 1 máquina", "PULADO", bl["skipped"])
    elif err("baseline_job") or not bl.get("speedup"):
        check("Desempenho vs. execução em 1 máquina", "FALHOU", err("baseline_job") or "sem resultado")
    else:
        check("Desempenho vs. execução em 1 máquina", "OK" if bl["speedup"] > 1.0 else "ATENÇÃO",
              f"speedup {bl['speedup']:.2f}x com {dj.get('n_islands')} nós "
              f"(eficiência {bl['efficiency'] * 100:.0f}%)")

    fj = r.get("fault_job") or {}
    fault = fj.get("fault") or {}
    job_ok = bool((fj.get("job") or {}).get("best_tour"))
    if fj.get("skipped"):
        check("Falha de trabalhador (detecção + recuperação)", "PULADO", fj["skipped"])
        check("Falha do líder (reeleição + continuidade)", "PULADO", fj["skipped"])
    elif err("fault_job"):
        check("Falha de trabalhador (detecção + recuperação)", "FALHOU", err("fault_job"))
        check("Falha do líder (reeleição + continuidade)", "FALHOU", err("fault_job"))
    else:
        wk = fj.get("worker")
        if fault.get("worker_skip"):
            check("Falha de trabalhador (detecção + recuperação)", "PULADO", fault["worker_skip"])
        elif wk:
            ok = job_ok and wk.get("detected_after_s") is not None
            check("Falha de trabalhador (detecção + recuperação)", "OK" if ok else "FALHOU",
                  f"nó {wk['node']} derrubado; detectado em {_s(wk.get('detected_after_s'))}, "
                  f"{len(wk.get('reassigned', []))} ilha(s) reatribuída(s); job concluído")
        else:
            check("Falha de trabalhador (detecção + recuperação)", "FALHOU", "falha não foi injetada")
        ld = fj.get("leader")
        if fault.get("leader_skip"):
            check("Falha do líder (reeleição + continuidade)", "PULADO", fault["leader_skip"])
        elif ld:
            ok = job_ok and ld.get("new") is not None
            check("Falha do líder (reeleição + continuidade)", "OK" if ok else "FALHOU",
                  f"líder {ld['old']} derrubado; nó {ld.get('new')} eleito em {_s(ld.get('failover_s'))} "
                  f"(mandato {ld.get('new_term')}); job concluído a partir do estado replicado")
        else:
            check("Falha do líder (reeleição + continuidade)", "FALHOU", "falha não foi injetada")

    cs = r.get("consistency") or {}
    if err("consistency") or not cs:
        check("Consistência das réplicas", "FALHOU", err("consistency") or "sem resultado")
    else:
        reps = cs.get("replicas", [])
        check("Consistência das réplicas", "OK" if cs.get("all_match") else "FALHOU",
              f"{sum(1 for x in reps if x.get('digest') == cs['leader_digest']) + 1}/{len(reps) + 1} "
              f"réplicas com o mesmo hash ({cs['leader_digest']}, versão {cs['version']})")

    lm = r.get("report") or {}
    if lm:
        ok = lm["violations"] == 0 and lm["pairs"] > 0
        check("Relógios de Lamport (ordem causal)", "OK" if ok else "FALHOU",
              f"{lm['pairs']} pares envio->recebimento ({lm['cross_node_pairs']} entre nós), "
              f"{lm['violations']} violações")

    # ---------------------------------------------------------------- texto
    bar = "=" * 78
    w(bar)
    w(f" ARQUIPÉLAGO — RELATÓRIO DO TESTE DISTRIBUÍDO  (teste {test.get('id')})")
    w(bar)
    dur = (test.get("finished_at") or 0) - (test.get("started_at") or 0)
    w(f"Início: {_fmt_t(test.get('started_at'))}   Duração: {int(dur // 60)} min {int(dur % 60)} s")
    initial = test.get("initial_members") or []
    w(f"Computadores no início do teste: {len(initial)}")
    for m in initial:
        tag = "  (líder)" if m["id"] == leader.get("id") else ""
        w(f"   nó {m['id']:>7}  {m.get('host', ''):<20} {m['address']}{tag}")
        if m.get("machine"):
            w(f"              {m['machine']}")
    w("")
    ok = sum(1 for c in checks if c["status"] == "OK")
    w(f"RESUMO: {ok}/{len(checks)} verificações OK")
    for i, c in enumerate(checks, 1):
        w(f"  [{c['status']:^7}] {i}. {c['name']}")
        w(f"            {c['detail']}")
    w("")

    if nodes:
        w("1. CONECTIVIDADE (gRPC, 5 pings por nó)")
        for n in nodes:
            rtt = (f"RTT médio {n['rtt_avg_ms']:.2f} ms (mín {n['rtt_min_ms']:.2f}, máx {n['rtt_max_ms']:.2f})"
                   if n["rtt_avg_ms"] is not None else "sem resposta")
            w(f"   nó {n['id']:>7} {n.get('host', ''):<20} {n['received']}/5  {rtt}")
        w("")

    hist = [h for h in con.get("history", []) if h["kind"] == "leader"]
    if hist:
        w("2. ELEIÇÃO DE LÍDER (Bully) — histórico até o início do teste")
        for h in hist:
            w(f"   {_fmt_t(h['t'])}  nó {h['node']} assumiu (mandato {h.get('term')}): {h.get('reason', '')}")
        w("")

    if bk and not err("berkeley"):
        w("3. RELÓGIOS FÍSICOS (Berkeley) — desvio em relação ao mestre (ms)")
        w(f"   {'nó':>8} {'antes':>11} {'ajuste':>11} {'depois':>9} {'RTT':>8} {'relógio do SO':>14}")
        for nid in sorted(bk["before_ms"], key=int):
            w(f"   {nid:>8} {bk['before_ms'][nid]:>+11.1f} {bk['adjust_ms'][nid]:>+11.1f} "
              f"{bk.get('after_ms', {}).get(nid, 0.0):>+9.2f} {bk['rtt_ms'].get(nid, 0):>8.2f} "
              f"{bk.get('os_offset_ms', {}).get(nid, 0.0):>+14.1f}")
        w("   (\"relógio do SO\" = diferença real entre os relógios dos computadores, sem o desvio")
        w("    artificial aplicado para a demonstração)")
        if bk.get("discarded"):
            w(f"   leituras descartadas como outliers: {bk['discarded']}")
        w("")

    if dj.get("best_tour"):
        w("4. AG DISTRIBUÍDO vs. 1 MÁQUINA")
        w(f"   instância: {len(dj['best_tour'])} cidades | população total {dj['pop_size']} | "
          f"{dj['epochs']} épocas x {dj['gens_per_epoch']} gerações")
        w(f"   distribuído ({dj['n_islands']} nós): {dj['wall_seconds']:7.2f} s   melhor rota {dj['best_length']:.1f}")
        if bl.get("wall_seconds"):
            w(f"   1 máquina           : {bl['wall_seconds']:7.2f} s   melhor rota {bl['best_length']:.1f}")
            w(f"   speedup = {bl['speedup']:.2f}x   eficiência = {bl['efficiency'] * 100:.0f}%")
        for isl in dj.get("islands", []):
            w(f"     ilha {isl['island']} no nó {isl['owner']}: melhor {isl['best_length']:.1f}, "
              f"cálculo {isl['compute_seconds']:.1f}s")
        w("")

    if fj and not fj.get("skipped") and not err("fault_job"):
        w("5. TOLERÂNCIA A FALHAS")
        job = fj.get("job") or {}
        wk, ld = fj.get("worker"), fj.get("leader")
        if wk:
            w(f"   Falha do trabalhador {wk['node']} ({wk.get('host')}):")
            w(f"     detectada pelo líder após {_s(wk.get('detected_after_s'))} (timeout de heartbeat)")
            for x in wk.get("reassigned", []):
                quem = "no próprio nó reiniciado" if x["to"] == x["from"] else f"no nó {x['to']}"
                w(f"     ilha {x['island']} retomada {quem} a partir do checkpoint da época {x['epoch']}")
            if wk.get("rejoined_after_s") is not None:
                w(f"     nó reiniciado voltou ao cluster após {_s(wk['rejoined_after_s'])}")
        if ld:
            w(f"   Falha do líder {ld['old']} ({ld.get('host')}, mandato {ld.get('old_term')}):")
            w(f"     novo líder: nó {ld.get('new')} (mandato {ld.get('new_term')}) após {_s(ld.get('failover_s'))}")
            for x in ld.get("reassigned", []):
                quem = "no próprio nó reiniciado" if x["to"] == x["from"] else f"no nó {x['to']}"
                w(f"     ilha {x['island']} retomada {quem} a partir do checkpoint replicado (época {x['epoch']})")
            if ld.get("rejoined_after_s") is not None:
                w(f"     antigo líder reiniciado voltou após {_s(ld['rejoined_after_s'])}")
            for x in ld.get("leaders_after", [])[1:]:
                w(f"     +{x['dt']:.1f}s: nó {x['node']} assumiu (mandato {x['term']}): {x.get('reason', '')}")
        if job:
            w(f"   job concluído: melhor rota {job['best_length']:.1f} em {job['wall_seconds']:.1f}s, "
              f"{len(job.get('reassignments', []))} reatribuição(ões) de ilha")
        w("")

    if cs:
        w("6. RÉPLICAS DO ESTADO DO COORDENADOR")
        w(f"   líder {cs['leader']}: versão {cs['version']}, mandato {cs['term']}, hash {cs['leader_digest']}")
        for x in cs.get("replicas", []):
            if x.get("reachable"):
                same = "igual" if x["digest"] == cs["leader_digest"] else "DIFERENTE"
                w(f"   nó {x['id']:>7}: versão {x['version']}, hash {x['digest']} ({same})")
            else:
                w(f"   nó {x['id']:>7}: inalcançável")
        w(f"   snapshots completos enviados: {cs.get('full_syncs')}, deltas: {cs.get('deltas')}")
        w("")

    if lm:
        w("7. RELÓGIOS DE LAMPORT")
        w(f"   {lm['events']} eventos de {lm['nodes']} nós; {lm['pairs']} pares envio->recebimento, "
          f"{lm['violations']} violações de L(envio) < L(recebimento)")
        w(f"   linha do tempo completa: {lm['file']} (no computador {lm.get('file_host')})")
        w("   amostra de mensagens entre nós diferentes (envio └─> recebimento):")
        for s in lm.get("sample", [])[:24]:
            w(f"     {s}")
        w("")
    w(bar)
    return "\n".join(lines), checks


def build_conclusion(test: dict) -> str:
    """Conclusão do teste em texto corrido (arquivo separado do relatório)."""
    r = test.get("results", {})
    checks = test.get("checks") or build_report(test, [])[1]
    pars = _conclusion(checks, test.get("initial_members") or [], r.get("connectivity") or {},
                       r.get("berkeley") or {}, r.get("dist_job") or {}, r.get("baseline_job") or {},
                       r.get("fault_job") or {}, r.get("consistency") or {}, r.get("report") or {})
    lines = [f"ARQUIPÉLAGO — CONCLUSÃO DO TESTE {test.get('id')}",
             f"Início: {_fmt_t(test.get('started_at'))}",
             f"Números detalhados: relatorio_teste_{test.get('id')}.txt (na mesma pasta)", ""]
    for par in pars:
        lines.append(textwrap.fill(par, width=78))
        lines.append("")
    return "\n".join(lines)


def _conclusion(checks, initial, con, bk, dj, bl, fj, cs, lm) -> list[str]:
    """Interpreta os resultados em linguagem natural (um parágrafo por tema)."""
    pars: list[str] = []
    ok = [c for c in checks if c["status"] == "OK"]
    bad = [c["name"] for c in checks if c["status"] == "FALHOU"]
    warn = [c["name"] for c in checks if c["status"] in ("PULADO", "ATENÇÃO")]
    hosts = {m.get("host") for m in initial if m.get("host")}
    n = len(initial)
    one_pc = len(hosts) <= 1

    onde = ("" if n == 1 else ", todos no mesmo computador (simulação local)" if one_pc
            else f", distribuídos em {len(hosts)} computadores diferentes")
    nos = "1 nó" if n == 1 else f"{n} nós"
    if not bad:
        geral = f"O sistema passou em {len(ok)} de {len(checks)} verificações, com {nos}{onde}."
    else:
        geral = (f"O sistema passou em {len(ok)} de {len(checks)} verificações, com {nos}{onde}. "
                 f"Não passaram: {'; '.join(bad)}. Veja os detalhes no relatório.")
    if warn:
        geral += f" Não puderam ser avaliadas por completo: {'; '.join(warn)}."
    pars.append(geral)

    nodes = con.get("nodes", [])
    avg = [x["rtt_avg_ms"] for x in nodes if x.get("rtt_avg_ms") is not None]
    if avg:
        rtt = sum(avg) / len(avg)
        pars.append(f"Comunicação: os nós trocaram mensagens gRPC com tempo médio de ida e volta de "
                    f"{rtt:.2f} ms" + (", valor baixo porque não há rede física entre eles."
                                       if one_pc else ", medido pela rede local."))

    if bk and not bk.get("error") and bk.get("spread_before_ms"):
        before, after = bk["spread_before_ms"], bk.get("spread_after_ms", 0)
        fator = f", cerca de {before / after:.0f} vezes menor" if after > 0 and before / after >= 2 else ""
        pars.append(f"Relógios físicos: o algoritmo de Berkeley reduziu a diferença entre os relógios "
                    f"de {before:.1f} ms para {after:.2f} ms{fator}. O mestre calcula a média dos "
                    f"desvios e cada nó se ajusta até ela.")

    if dj.get("best_tour"):
        txt = (f"Algoritmo genético: as {dj['n_islands']} ilhas evoluíram em paralelo e chegaram a uma "
               f"rota de comprimento {dj['best_length']:.1f} em {dj['wall_seconds']:.1f} s.")
        if bl.get("speedup"):
            sp, ef = bl["speedup"], bl["efficiency"] * 100
            txt += (f" Em uma única máquina, o mesmo trabalho levou {bl['wall_seconds']:.1f} s, então o "
                    f"sistema distribuído foi {sp:.2f} vezes mais rápido (eficiência de {ef:.0f}%).")
            if one_pc and ef < 90:
                txt += (" O ganho não é linear porque todos os nós disputam o mesmo processador; em "
                        "computadores separados ele tende a ser maior.")
            elif ef < 90:
                txt += " O ganho não é linear por causa da migração entre ilhas e da coordenação pelo líder."
            dif = (bl["best_length"] - dj["best_length"]) / bl["best_length"] * 100
            if dif > 0.05:
                txt += (f" A versão distribuída também achou uma rota {dif:.1f}% mais curta, porque ilhas "
                        f"separadas mantêm mais diversidade genética.")
            elif dif < -0.05:
                txt += (f" A rota da versão distribuída ficou {-dif:.1f}% mais longa; o AG é aleatório e "
                        f"essa diferença varia de uma execução para outra.")
        pars.append(txt)

    wk, ld = fj.get("worker"), fj.get("leader")
    job = fj.get("job") or {}
    if wk or ld:
        partes = []
        if wk and wk.get("detected_after_s") is not None:
            k = len(wk.get("reassigned", []))
            ilhas = "1 ilha foi retomada" if k == 1 else f"{k} ilhas foram retomadas"
            partes.append(f"a queda de um trabalhador foi detectada em {wk['detected_after_s']:.1f} s e "
                          f"{ilhas} a partir do último checkpoint, sem recomeçar do zero")
        if ld and ld.get("new") is not None:
            partes.append(f"a queda do líder levou a uma nova eleição Bully em "
                          f"{ld.get('failover_s') or 0:.1f} s (nó {ld['new']}, mandato {ld.get('new_term')}), "
                          f"e o novo líder continuou o trabalho usando o estado replicado")
        if partes:
            txt = "Tolerância a falhas: " + "; ".join(partes) + "."
            if job.get("best_tour"):
                txt += (f" Mesmo com as falhas, o job terminou com uma rota válida "
                        f"({job['best_length']:.1f}).")
            pars.append(txt)

    if cs:
        reps = cs.get("replicas", [])
        iguais = sum(1 for x in reps if x.get("digest") == cs.get("leader_digest")) + 1
        if cs.get("all_match"):
            pars.append(f"Réplicas: ao final, as {iguais} cópias do estado do coordenador eram idênticas "
                        f"(mesma versão e mesmo hash), o que permite que qualquer nó assuma como líder.")
        else:
            pars.append(f"Réplicas: apenas {iguais} de {len(reps) + 1} cópias do estado do coordenador "
                        f"estavam iguais ao final do teste.")

    if lm and lm.get("pairs"):
        if lm["violations"] == 0:
            pars.append(f"Relógios lógicos: em todos os {lm['pairs']} pares de envio e recebimento, o "
                        f"relógio de Lamport do envio foi menor que o do recebimento, ou seja, a ordem "
                        f"causal foi respeitada mesmo quando os relógios físicos discordavam.")
        else:
            pars.append(f"Relógios lógicos: foram encontradas {lm['violations']} violações da ordem "
                        f"causal em {lm['pairs']} pares de envio e recebimento.")

    if not bad and not warn:
        pars.append("Em resumo, o sistema distribuiu o algoritmo genético entre os nós, ficou mais rápido "
                    "que uma única máquina e continuou funcionando quando um trabalhador e o próprio "
                    "líder caíram.")
    return pars
