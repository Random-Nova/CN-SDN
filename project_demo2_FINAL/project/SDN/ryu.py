cat /home/rhithvi/controller.py
import time

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import (
    CONFIG_DISPATCHER,
    MAIN_DISPATCHER,
    set_ev_cls
)
from ryu.ofproto import ofproto_v1_3
from ryu.lib import hub
from ryu.lib.packet import packet, ethernet, arp


# ---------------------------------------------------------------
# Port numbers (match the order links are added in topo.py)
#   s1: 1-3 = c1,c2,c3   4 = s2   5 = s4
#   s2: 1 = srv          2 = s1   3 = s3
#   s3: 1-3 = w1,w2,w3   4 = s2   5 = s4
#   s4: 1 = s1           2 = s3
# ---------------------------------------------------------------

# PRIMARY paths: clients - s1 - s2 - s3 - workers, server on s2
ROUTES = {
    1: [
        ("10.0.0.1", 1), ("10.0.0.2", 2), ("10.0.0.3", 3),
        ("10.0.0.10", 4),
        ("10.0.0.21", 4), ("10.0.0.22", 4), ("10.0.0.23", 4),
    ],
    2: [
        ("10.0.0.1", 2), ("10.0.0.2", 2), ("10.0.0.3", 2),
        ("10.0.0.10", 1),
        ("10.0.0.21", 3), ("10.0.0.22", 3), ("10.0.0.23", 3),
    ],
    3: [
        ("10.0.0.1", 4), ("10.0.0.2", 4), ("10.0.0.3", 4),
        ("10.0.0.10", 4),
        ("10.0.0.21", 1), ("10.0.0.22", 2), ("10.0.0.23", 3),
    ],
    4: [
        ("10.0.0.1", 1), ("10.0.0.2", 1), ("10.0.0.3", 1),
        ("10.0.0.10", 1),
        ("10.0.0.21", 2), ("10.0.0.22", 2), ("10.0.0.23", 2),
    ],
}

# BACKUP paths, used when the s2-s3 link is down.
# Only the entries that CHANGE are listed. Everything else stays.
#   srv/clients -> workers:  s2 -> s1 -> s4 -> s3
#   workers -> srv/clients:  s3 -> s4 -> s1 -> (s2 -> srv)
BACKUP_ROUTES = {
    1: [   # s1: send worker traffic to s4 (port 5) instead of s2
        ("10.0.0.21", 5), ("10.0.0.22", 5), ("10.0.0.23", 5),
    ],
    2: [   # s2: send worker traffic back up to s1 (port 2)
        ("10.0.0.21", 2), ("10.0.0.22", 2), ("10.0.0.23", 2),
    ],
    3: [   # s3: send server/client traffic to s4 (port 5) instead of s2
        ("10.0.0.1", 5), ("10.0.0.2", 5), ("10.0.0.3", 5),
        ("10.0.0.10", 5),
    ],
}

# If either end of the s2-s3 link goes down, the primary path is broken
PRIMARY_LINK_PORTS = {(2, 3), (3, 4)}

PORT_NAMES = {
    (1, 1): "s1->c1",  (1, 2): "s1->c2",  (1, 3): "s1->c3",
    (1, 4): "s1->s2",  (1, 5): "s1->s4",
    (2, 1): "s2->srv", (2, 2): "s2->s1",  (2, 3): "s2->s3",
    (3, 1): "s3->w1",  (3, 2): "s3->w2",  (3, 3): "s3->w3",
    (3, 4): "s3->s2",  (3, 5): "s3->s4",
    (4, 1): "s4->s1",  (4, 2): "s4->s3",
}

LINK_CAPACITY_MBPS = 10.0
CONGESTION_THRESHOLD = 0.8
POLL_INTERVAL = 2
MIN_REPORT_MBPS = 0.05


class JobSDN(app_manager.RyuApp):

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datapaths = {}          # switch id -> datapath
        self.prev_stats = {}         # (dpid, port) -> (total_bytes, time)
        self.link_load = {}          # (dpid, port) -> latest Mbps
        self.down_ports = set()      # (dpid, port) currently down
        self.using_backup = False
        self.monitor_thread = hub.spawn(self._monitor)

    # ---------- switch connects ----------
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def features_handler(self, ev):
        dp = ev.msg.datapath
        parser = dp.ofproto_parser
        ofp = dp.ofproto

        self.datapaths[dp.id] = dp

        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofp.OFPP_CONTROLLER,
                                          ofp.OFPCML_NO_BUFFER)]
        self.add_flow(dp, 0, match, actions)

        self.logger.info("Switch connected: s%s", dp.id)
        self.install_routes(dp, ROUTES)

    # ---------- helpers ----------
    def add_flow(self, dp, priority, match, actions):
        parser = dp.ofproto_parser
        ofp = dp.ofproto
        instructions = [parser.OFPInstructionActions(
            ofp.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=dp,
                                priority=priority,
                                match=match,
                                instructions=instructions)
        dp.send_msg(mod)

    def install_routes(self, dp, routes):
        """Install (dst_ip, out_port) rules on one switch.
        A rule with the same match and priority REPLACES the old one."""
        parser = dp.ofproto_parser
        for dst_ip, out_port in routes.get(dp.id, []):
            match = parser.OFPMatch(eth_type=0x0800, ipv4_dst=dst_ip)
            actions = [parser.OFPActionOutput(out_port)]
            self.add_flow(dp, 10, match, actions)
        self.logger.info("Routes installed on s%s", dp.id)

    # ---------- ARP (only used if static ARP is NOT set) ----------
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        dp = msg.datapath
        ofp = dp.ofproto
        parser = dp.ofproto_parser

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        arp_pkt = pkt.get_protocol(arp.arp)

        if eth is None or arp_pkt is None:
            return

        self.logger.info("ARP packet-in on s%s (static ARP not working?)",
                         dp.id)
        if arp_pkt.opcode == arp.ARP_REQUEST:
            actions = [parser.OFPActionOutput(ofp.OFPP_FLOOD)]
            out = parser.OFPPacketOut(datapath=dp,
                                      buffer_id=ofp.OFP_NO_BUFFER,
                                      in_port=msg.match["in_port"],
                                      actions=actions,
                                      data=msg.data)
            dp.send_msg(out)

    # ---------- Step 9: failure detection and rerouting ----------
    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def port_status_handler(self, ev):
        """A switch tells us one of its ports changed state."""
        msg = ev.msg
        dp = msg.datapath
        ofp = dp.ofproto
        port = msg.desc.port_no
        key = (dp.id, port)
        name = PORT_NAMES.get(key, "s%s port %s" % key)

        is_down = bool(msg.desc.state & ofp.OFPPS_LINK_DOWN) or \
            bool(msg.desc.config & ofp.OFPPC_PORT_DOWN)

        if is_down:
            if key not in self.down_ports:
                self.logger.warning("LINK DOWN: %s", name)
            self.down_ports.add(key)
        else:
            if key in self.down_ports:
                self.logger.info("LINK UP: %s", name)
            self.down_ports.discard(key)

        self.update_paths()

    def update_paths(self):
        primary_broken = any(k in self.down_ports
                             for k in PRIMARY_LINK_PORTS)

        if primary_broken and not self.using_backup:
            self.using_backup = True
            self.logger.warning(
                "REROUTING: primary path s2-s3 is down, "
                "switching to backup via s1-s4")
            self.apply_routes(BACKUP_ROUTES)

        elif not primary_broken and self.using_backup:
            self.using_backup = False
            self.logger.info(
                "RESTORING: primary path s2-s3 is back, "
                "returning to primary routes")
            self.apply_routes(ROUTES)

    def apply_routes(self, routes):
        for dpid, dp in list(self.datapaths.items()):
            self.install_routes(dp, routes)

    # ---------- Step 8: load monitoring ----------
    def _monitor(self):
        while True:
            for dp in list(self.datapaths.values()):
                parser = dp.ofproto_parser
                req = parser.OFPPortStatsRequest(
                    dp, 0, dp.ofproto.OFPP_ANY)
                dp.send_msg(req)
            hub.sleep(POLL_INTERVAL)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats_handler(self, ev):
        dpid = ev.msg.datapath.id
        now = time.time()

        for stat in ev.msg.body:
            port = stat.port_no
            if port > 0xffffff00:        # skip the internal LOCAL port
                continue

            key = (dpid, port)
            total_bytes = stat.rx_bytes + stat.tx_bytes

            if key in self.prev_stats:
                old_bytes, old_time = self.prev_stats[key]
                dt = now - old_time
                if dt > 0:
                    mbps = (total_bytes - old_bytes) * 8 / dt / 1e6
                    self.link_load[key] = mbps
                    self.report(key, mbps)

            self.prev_stats[key] = (total_bytes, now)

    def report(self, key, mbps):
        if mbps < MIN_REPORT_MBPS:
            return
        name = PORT_NAMES.get(key, "s%s port %s" % key)
        percent = mbps / LINK_CAPACITY_MBPS * 100
        if mbps > LINK_CAPACITY_MBPS * CONGESTION_THRESHOLD:
            self.logger.warning("CONGESTED %s: %.2f Mbps (%.0f%%)",
                                name, mbps, percent)
        else:
            self.logger.info("LOAD %s: %.2f Mbps (%.0f%%)",
                             name, mbps, percent)