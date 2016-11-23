# MOEXmcTEST
MOEX Multicast FAST Listener

Small tool for cheking MOEX Multicast FAST feeds.

You need to define configuration xml file as a parameter of this script

All configuration files you may find on ftp://ftp.moex.com/pub/FAST/

# Example of ussage
Listening all feeds

./mmctest.py config_production.xml

Listening only one source

./mmctest.py config_production.xml 225

Listening only A feeds

./mmctest.py config_production.xml 253

Listening only snapshots

./mmctest.py config_production.xml Snap

Or only one groupe (by UDP port)

./mmctest.py config_production.xml 16001
