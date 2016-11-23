#!/usr/bin/python

#####################################################
#
# MOEX FIX/FAST Listener
# 
# Version: 0.1
#
#####################################################

import os
import sys
import time
import copy
import socket
import select

from xml.dom.minidom import parse
import xml.dom.minidom

def pars_config( file, udp, tcp ):

  DOMTree = xml.dom.minidom.parse(file)
  cfg = DOMTree.documentElement
  
  if cfg.hasAttribute("environment"):
    print "#"*50,"\nASTS config\n","#"*50
  
    connections = cfg.getElementsByTagName("connection")
    if connections:
      for connection in connections:

        row = {}

        row['feedName']  = connection.getAttribute("id").strip()
        row['feedLabel'] = connection.getElementsByTagName("type").item(0).getAttribute("feed-type").strip()
        row['protocol'] = connection.getElementsByTagName("protocol").item(0).firstChild.nodeValue
        
        if 'TCP' in row['protocol'].upper():
          row['src-ip'] = ''
          row['port']   = connection.getElementsByTagName("port").item(0).firstChild.nodeValue
          # get first IP
          row['ip']     = connection.getElementsByTagName("ip").item(0).firstChild.nodeValue
          tcp.append( row )
          
          row2 = copy.deepcopy(row)
          # get second IP
          row2['ip']     = connection.getElementsByTagName("ip").item(1).firstChild.nodeValue
          tcp.append( row2 )
          continue

        row['feedType'] = ""
        #row['feedType'] = connection.getElementsByTagName("feed").item(0).getAttribute("id").strip()
        row['src-ip']   = connection.getElementsByTagName("feed").item(0).getElementsByTagName("src-ip").item(0).firstChild.nodeValue
        row['ip']       = connection.getElementsByTagName("feed").item(0).getElementsByTagName("ip").item(0).firstChild.nodeValue
        row['port']     = connection.getElementsByTagName("feed").item(0).getElementsByTagName("port").item(0).firstChild.nodeValue
        
        udp.append( row )

        row2 = copy.deepcopy( row )
        row2['feedType'] = ""
        #row2['feedType'] = connection.getElementsByTagName("feed").item(1).getAttribute("id").strip()                     
        row2['src-ip']   = connection.getElementsByTagName("feed").item(1).getElementsByTagName("src-ip").item(0).firstChild.nodeValue
        row2['ip']       = connection.getElementsByTagName("feed").item(1).getElementsByTagName("ip").item(0).firstChild.nodeValue
        row2['port']     = connection.getElementsByTagName("feed").item(1).getElementsByTagName("port").item(0).firstChild.nodeValue

        udp.append( row2 )
  
  elif cfg.hasAttribute("type"):
    print "#"*50,"\nSPECTRA config\n","#"*50
    
    MarketDataGroups = cfg.getElementsByTagName("MarketDataGroup")
    if MarketDataGroups:
      for MarketDataGroup in MarketDataGroups:
        
        rowb = {}
        
        rowb['feedName']  = MarketDataGroup.getAttribute("feedType").strip()
        rowb['feedLabel'] = MarketDataGroup.getAttribute("label").strip()
        connections = MarketDataGroup.getElementsByTagName("connection")
        if connections:
          for connection in connections:
            
            row = copy.deepcopy( rowb )

            row['feedType'] = connection.getElementsByTagName("type").item(0).firstChild.nodeValue
            row['protocol'] = connection.getElementsByTagName("protocol").item(0).firstChild.nodeValue
            row['ip']       = connection.getElementsByTagName("ip").item(0).firstChild.nodeValue
            row['port']     = connection.getElementsByTagName("port").item(0).firstChild.nodeValue
            if 'TCP' in row['protocol'].upper():
              row['src-ip'] = ''
              row['feed']   = ''
              tcp.append( row )
              continue
            row['src-ip']   = connection.getElementsByTagName("src-ip").item(0).firstChild.nodeValue
            row['feed']     = connection.getElementsByTagName("feed").item(0).firstChild.nodeValue
            udp.append( row ) 

  else: 
    print "UNKNOWN config"
    sys.exit(0)

def create_ssm_listener( src, grp, port ):
  imr = (socket.inet_pton(socket.AF_INET, grp) +
         socket.inet_pton(socket.AF_INET, '0.0.0.0') +
         socket.inet_pton(socket.AF_INET, src))
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
  
  # Buffer size
  s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16777216 )
  
  # SSM option
  s.setsockopt(socket.SOL_IP, socket.IP_ADD_SOURCE_MEMBERSHIP, imr)
  
  # allows reuse address
  s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  
  #allows reuse port (multiple listeners for one groupe simultaneously)
  if hasattr(socket, "SO_REUSEPORT"):
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    
  s.bind((grp, int(port)))
  return s

def clearScreen():
  print chr(27)+"[2J"+chr(27)+"[;H",                    # clear ANSI screen (thanks colorama for windows)  
         
def main():
  os.chdir(os.path.dirname(os.path.realpath(sys.argv[0])))
  
  if len(sys.argv)==1:
    print ""
    print "Usage: mctest.py <configfilename> [<filter>]"
    print ""
    sys.exit(0)

  udp = []
  tcp = []
  
  pars_config( sys.argv[1], udp, tcp )

  ofile = sys.argv[1].lower()
  if ofile[-4:] == '.xml':
    ofile = ofile[:-4]+'.txt'
  else:
    ofile = ofile+'.txt'

  filter = '{'  #will shows all feeds
  if len(sys.argv)>2:
    filter = sys.argv[2] 
     
  sockets = []
  l_feeds = {}  #listened feeds  

  if not hasattr(socket, 'IP_MULTICAST_TTL'):
    setattr(socket, 'IP_MULTICAST_TTL', 33)
  if not hasattr(socket, 'IP_ADD_SOURCE_MEMBERSHIP'):
    setattr(socket, 'IP_ADD_SOURCE_MEMBERSHIP', 39)

  # start listening
  curt = time.time()
  for feed in udp:
    if filter in str(feed):
      print "Subscribing to :",feed['src-ip'], feed['ip'], int(feed['port'])
      s = create_ssm_listener( feed['src-ip'], feed['ip'], int(feed['port']) )
      sockets.append(s)
      l_port = int(feed['port'])
      l_feeds[l_port] = feed
      l_feeds[l_port]['pkts']     = 0
      l_feeds[l_port]['bytes']    = 0
      l_feeds[l_port]['lastseq']  = 0
      l_feeds[l_port]['lasttime'] = curt
      l_feeds[l_port]['loss']     = 0

  starttime = time.time()
  last_shown_time = 0
  empty = []
  fo = 0

  # main loop
  while True:
    readable, writable, exceptional = select.select(sockets, empty, empty)
    curt = time.time()

    for s in readable:
      feed_data = s.recvfrom(2048)
      feed_port = s.getsockname()[1]
     
      if curt-starttime<1:
        continue

      # get sequence
      seq = 0
      for i in range(0,4):
        seq = seq*256 + ord(feed_data[0][3-i])

      # sequence analysis
      if l_feeds[feed_port]['pkts']==0:
        l_feeds[feed_port]['lastseq']=seq
      if seq>(l_feeds[feed_port]['lastseq']+1):
        l_feeds[feed_port]['loss'] += (seq-l_feeds[feed_port]['lastseq']-1)
      l_feeds[feed_port]['lastseq'] = seq

      l_feeds[feed_port]['pkts'] += 1
      l_feeds[feed_port]['bytes'] += len(feed_data[0])
      l_feeds[feed_port]['lasttime'] = time.time()

    if time.time()-last_shown_time>0.2:
      clearScreen()

      print "%-15s|%-16s|%-50s|%7s|%10s|%7s|%10s" % ("Source","Group","Label","Packets","Bytes","Loss","SecondsAgo")

      for l in sorted(l_feeds.keys()):
        st = "%-16s|%-16s|%-50s|%7d|%10d|%7d|%10d\n" % (l_feeds[l]['src-ip'],l_feeds[l]['ip'],l_feeds[l]['feedName']+' '+l_feeds[l]['feedLabel'],l_feeds[l]['pkts'],l_feeds[l]['bytes'],l_feeds[l]['loss'],(curt-l_feeds[l]['lasttime']))
        print st,
        
      # renew counters at 03:00 every day
      if time.gmtime(last_shown_time).tm_hour==2 and time.gmtime(curt).tm_hour==3:
        for l in l_feeds.keys():
          l_feeds[l]['pkts']     = 0
          l_feeds[l]['bytes']    = 0
          l_feeds[l]['lasttime'] = curt
          l_feeds[l]['loss']     = 0

      last_shown_time = curt

  for s in sockets:
    s.close()
   
if __name__ == "__main__":
  main()

