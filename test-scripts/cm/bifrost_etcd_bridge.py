#!/usr/bin/env python

import argparse
import socket
import glob
import os
import time

import simplejson as json
import etcd3 as etcd
from bifrost.proclog import load_by_pid

BIFROST_STATS_BASE_DIR = '/dev/shm/bifrost/'

def get_command_line(pid):
    """
    Given a PID, use the /proc interface to get the full command line for 
    the process.  Return an empty string if the PID doesn't have an entry in
    /proc.
    """

    cmd = ''
    try:
        with open('/proc/%i/cmdline' % pid, 'r') as fh:
            cmd = fh.read()
            cmd = cmd.replace('\0', ' ')
            fh.close()
    except IOError:
        pass
    return cmd

def poll(base_dir):
    ## Find all running processes
    pidDirs = glob.glob(os.path.join(base_dir, '*'))
    pidDirs.sort()

    ## Load the data
    blockList = {}
    for pn, pidDir in enumerate(pidDirs):
        pid = int(os.path.basename(pidDir), 10)
        contents = load_by_pid(pid)

        cmd = get_command_line(pid)
        if cmd == '':
            continue

        for block in contents.keys():
            try:
                log = contents[block]['bind']
                cr = log['core0']
            except KeyError:
                continue

            try:
                pipeline_id = contents['block']['id']
            except KeyError:
                pipeline_id = pn

            try:
                log = contents[block]['perf']
                ac = max([0.0, log['acquire_time']])
                pr = max([0.0, log['process_time']])
                re = max([0.0, log['reserve_time']])
                gb = max([0.0, log.get('gbps', 0.0)])
            except KeyError:
                ac, pr, re, gb = 0.0, 0.0, 0.0, 0.0

            blockList['%i-%s' % (pipeline_id, block)] = {'pid': pid, 'name':block, 'cmd': cmd, 'core': cr, 'acquire': ac, 'process': pr, 'reserve': re, 'total':ac+pr+re, 'gbps':gb, 'time':time.time()}

            try:
                log = contents[block]['sequence0']
                blockList['%i-%s' % (pipeline_id, block)].update(log)
            except:
                pass


            # Get UDP stats if appropriate
            if block[:3] == 'udp':
                try:
                    log     = contents[block]['stats']
                    good    = log['ngood_bytes']
                    missing = log['nmissing_bytes']
                    invalid = log['ninvalid_bytes']
                    late    = log['nlate_bytes']
                    nvalid  = log['nvalid']
                except KeyError:
                    good, missing, invalid, late, nvalid = 0, 0, 0, 0, 0
                netstats = {'good': good, 'missing': missing,
                            'invalid': invalid, 'late': late,
                            'nvalid': nvalid}
                blockList['%i-%s' % (pipeline_id, block)]['netstats'] = netstats

    return time.time(), blockList

def main(args):
   ec = etcd.client(args.etcdhost)
   last_poll = 0
   while True:
       try:
           wait_time = max(0, last_poll + args.polltime - time.time())
           time.sleep(wait_time)
           last_poll, d = poll(BIFROST_STATS_BASE_DIR)
           for k, v in d.items():
               pipeline_id, block = k.split('-')
               ekey = '{keybase}/x/{hostbase}/pipeline/{pipeline_id}/{block}'.format(
                          keybase=args.keybase,
                          hostbase=args.hostbase,
                          pipeline_id=pipeline_id,
                          block=block
                      )
               ec.put(ekey, json.dumps(v))
           
       except KeyboardInterrupt:
           break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Display perfomance of different blocks of Bifrost pipelines',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('--etcdhost', default='localhost',
                        help='etcd host to which stats should be published')
    parser.add_argument('--keybase', default='/mon/corr',
                        help='Key to which stats should be published: <keybase>/x/<hostbase>/pipeline/<pipeline-id>/blockname/...')
    parser.add_argument('--hostbase', default=socket.gethostname(),
                        help='Key to which stats should be published: <keybase>/x/<hostbase>/pipeline/<pipeline-id>/blockname/...')
    parser.add_argument('-t', '--polltime', type=int, default=10,
                        help='How often to poll stats, in seconds')
    args = parser.parse_args()
    main(args)
