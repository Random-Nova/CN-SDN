cat /home/rhithvi/topo.py
#!/usr/bin/env python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import setLogLevel

def build():
    net = Mininet(controller=RemoteController, switch=OVSSwitch,
                  link=TCLink, autoSetMacs=True)

    c0 = net.addController('c0', ip='127.0.0.1', port=6633)

    # Switches (OpenFlow 1.3)
    s1 = net.addSwitch('s1', protocols='OpenFlow13')
    s2 = net.addSwitch('s2', protocols='OpenFlow13')
    s3 = net.addSwitch('s3', protocols='OpenFlow13')
    s4 = net.addSwitch('s4', protocols='OpenFlow13')

    # Hosts
    clients = [net.addHost(f'c{i}', ip=f'10.0.0.{i}/24') for i in (1, 2, 3)]
    server  = net.addHost('srv', ip='10.0.0.10/24')
    workers = [net.addHost(f'w{i}', ip=f'10.0.0.{20+i}/24') for i in (1, 2, 3)]

    # Clients -> s1
    for c in clients:
        net.addLink(c, s1)
    # Server -> s2
    net.addLink(server, s2)
    # Workers -> s3
    for w in workers:
        net.addLink(w, s3)

    # Switch links: primary path s1-s2-s3, backup path s1-s4-s3
    net.addLink(s1, s2, bw=10)
    net.addLink(s2, s3, bw=10)
    net.addLink(s1, s4, bw=10)
    net.addLink(s4, s3, bw=10)

    net.start()
    net.staticArp()
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    build()