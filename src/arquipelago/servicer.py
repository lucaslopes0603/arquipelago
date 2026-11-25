"""Implementação do serviço gRPC: cada RPC é delegado ao componente responsável."""

import json
import logging
import threading

from .events import read_all
from .rpc import arquipelago_pb2 as pb
from .rpc import arquipelago_pb2_grpc as pbg

log = logging.getLogger("arquipelago")


class NodeServicer(pbg.ArquipelagoNodeServicer):
    def __init__(self, node):
        self.node = node

    # pertencimento / eleição
    def Join(self, request, context):
        return self.node.membership.on_join(request, context)

    def Heartbeat(self, request, context):
        return self.node.membership.on_heartbeat(request, context)

    def Election(self, request, context):
        return self.node.election.on_election(request, context)

    def Coordinator(self, request, context):
        return self.node.election.on_coordinator(request, context)

    def Ping(self, request, context):
        node = self.node
        node.recv(request.h)
        return pb.PingReply(h=node.hdr(), node=node.info(), leader_id=node.leader_id or 0)

    def AddPeer(self, request, context):
        self.node.add_seed(request.address.strip())
        self.node.local_log(f"Endereço adicionado manualmente: {request.address}")
        return pb.Ack(ok=True)

    # relógios
    def GetTime(self, request, context):
        return self.node.berkeley.on_get_time(request, context)

    def AdjustTime(self, request, context):
        return self.node.berkeley.on_adjust(request, context)

    # replicação
    def Replicate(self, request, context):
        return self.node.replicator.on_replicate(request, context)

    # algoritmo genético
    def SubmitJob(self, request, context):
        node = self.node
        node.recv(request.h)
        if not node.is_leader:
            fwd = pb.JobRequest(h=node.hdr(), spec_json=request.spec_json)
            rep = node.send_to_leader("SubmitJob", fwd, timeout=5.0)
            return rep or pb.JobReply(h=node.hdr(), ok=False, info="líder desconhecido")
        ok, info = node.jobs.submit(json.loads(request.spec_json))
        return pb.JobReply(h=node.hdr(), ok=ok, job_id=info if ok else "", info="" if ok else info,
                           leader_id=node.id, leader_address=node.address)

    def AssignIsland(self, request, context):
        return self.node.jobs.on_assign(request, context)

    def Migrate(self, request, context):
        return self.node.jobs.on_migrate(request, context)

    def ReportProgress(self, request, context):
        return self.node.jobs.on_report(request, context)

    def CancelJob(self, request, context):
        return self.node.jobs.on_cancel(request, context)

    # teste / operação
    def StartTest(self, request, context):
        node = self.node
        node.recv(request.h)
        if not node.is_leader:
            fwd = pb.StartTestRequest(h=node.hdr(), requested_by=request.requested_by,
                                      options_json=request.options_json)
            rep = node.send_to_leader("StartTest", fwd, timeout=5.0)
            return rep or pb.Ack(h=node.hdr(), ok=False, info="líder desconhecido")
        options = json.loads(request.options_json) if request.options_json else {}
        ok, info = node.orchestrator.start_test(request.requested_by, options)
        return pb.Ack(h=node.hdr(), ok=ok, info=info)

    def InjectCrash(self, request, context):
        node = self.node
        node.recv(request.h)
        node.local_log(f"Recebi ordem de FALHA SIMULADA: {request.reason}")
        # Responde primeiro e morre logo depois (a resposta precisa sair do processo).
        threading.Timer(0.3, node.crash, args=(request.reason,)).start()
        return pb.Ack(h=node.hdr(), ok=True)

    def GetStatus(self, request, context):
        return pb.JsonReply(json=json.dumps(self.node.status(), default=str))

    def GetEvents(self, request, context):
        node = self.node
        return pb.JsonReply(json=json.dumps(read_all(node.data_dir, node.id)))
