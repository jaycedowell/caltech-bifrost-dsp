import argparse
import time
import sys
import logging
import simplejson as json
import etcd3 as etcd

default_log = logging.getLogger(__name__)
logFormat = logging.Formatter('%(asctime)s [%(levelname)-8s] %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
logFormat.converter = time.gmtime
logHandler = logging.StreamHandler(sys.stdout)
logHandler.setFormatter(logFormat)
default_log.addHandler(logHandler)

class EtcdCorrControl():
    """
    **Description**

    A class to encapsulate the LWA correlator etcd-based control protocol,
    whereby commands are sent to processing blocks in the LWA pipeline
    by writing json-encoded strings to an appropriate etcd key.

    The target key for a particular processing block is defined as:

    ``<command-root>/<hostname>/pipeline/<pipeline-id>/<blockname>/<block-id>/ctrl``

    Where:
      -  ``command-root`` is a user-defined root path
      -  ``hostname`` is the hostname of the server running the DSP pipeline
      -  ``pipeline-id`` is the index of the DSP pipeline on this server
      -  ``blockname`` is the name of the processing block in the pipeline
      -  ``block-id`` is the index of the instance of this block type

    Similarly, a processing block's status can be read by reading the key:

    ``<monitor-root>/<hostname>/pipeline/<pipeline-id>/<blockname>/<block-id>/status``

    Where:
      -  ``monitor-root`` is a user-defined root path
      -  Other key elements are defined as per the command key above

    This class facilitates the generation of appropriate keys given a
    target host/pipeline/block, and handles interactions with the correlator's
    etcd server as well as the encoding and decoding of JSON messages.

    **Instantiation**

    :param etcdhost: The hostname of the system running the correlator's
        etcd server
    :type etcdhost: string

    :param keyroot_cmd: The root path under which all correlator command
        keys live
    :type keyroot_cmd: string

    :param keyroot_mon: The root path under which all correlator monitor
        keys live
    :type keyroot_mon: string

    :param log: The logger to which this class should emit log messages.
        The default behaviour is to log to stdout
    :type log: logging.Logger

    """
    def __init__(self, etcdhost='etcdhost', keyroot_cmd='/cmd/corr/x',
                 keyroot_mon='/mon/corr/x', keyroot_resp='/resp/corr/x',
                 log=default_log):
        self.keyroot_cmd = keyroot_cmd
        self.keyroot_mon = keyroot_mon
        self.keyroot_resp = keyroot_resp
        self.etcdhost = etcdhost
        self.log = log
        try:
            self.ec = etcd.client(self.etcdhost)
        except:
            log.error('Failed to connect to ETCD host %s' % self.etcdhost)
            raise

    def _get_cmd_key(self, host, pipeline, block, inst_id):
        """
        Generate a block's command key from the block instance specification.

        :param host: The hostname of the server running the DSP pipeline
            to be commanded
        :type host: string
        :param pipeline: The index of the pipeline on this server to be
            commanded
        :type pipeline: int
        :param block: The name of the processing block in this pipeline
            to be commanded
        :type block: string
        :param inst_id: The instance ID of the block of this type to be
            commanded
        :type inst_id: int

        :return: The command key for this block
        :rtype: string

        """

        key = '/%s/pipeline/%d/%s/%d/ctrl' % (host, pipeline, block, inst_id)
        return self.keyroot_cmd + key

    def _get_mon_key(self, host, pipeline, block, inst_id):
        """
        Generate a block's monitor key from the block instance specification.

        :param host: The hostname of the server running the DSP pipeline
            to be commanded
        :type host: string
        :param pipeline: The index of the pipeline on this server to be
            commanded
        :type pipeline: int
        :param block: The name of the processing block in this pipeline
            to be commanded
        :type block: string
        :param inst_id: The instance ID of the block of this type to be
            commanded
        :type inst_id: int

        :return: The monitor key for this block
        :rtype: string

        """

        key = '/%s/pipeline/%d/%s/%d/status' % (host, pipeline, block, inst_id)
        return self.keyroot_mon + key

    def send_command(self, host, pipeline, block, inst_id, **kwargs):
        """
        Send a command to a processing block

        :param host: The hostname of the server running the DSP pipeline
            to be commanded
        :type host: string
        :param pipeline: The index of the pipeline on this server to be
            commanded
        :type pipeline: int
        :param block: The name of the processing block in this pipeline
            to be commanded
        :type block: string
        :param inst_id: The instance ID of the block of this type to be
            commanded
        :type inst_id: int

        :param **kwargs: Keyword arguments are used to specify which
            control values should be set. Any key names and JSON-serializable
            values are allowed. These should match the key names expected
            by the processing block being targeted.
        :type **kwargs: Any JSON-serializable values

        """

        key = self._get_cmd_key(host, pipeline, block, inst_id)
        val = json.dumps(kwargs)
        self.ec.put(key, val)

    def _send_command_etcd(self, xhost, block, cmd, kwargs={}, timeout=10.0):
        """
        Send a command to an X-Engine

        :param xhost: X-engine hostname to which command should be sent.
        :type xhost: int

        :param block: Block to which command applies.
        :type block: str

        :param cmd: Command to be sent
        :type cmd: str

        :param kwargs: Dictionary of key word arguments to be forwarded
            to the chosen command method
        :type kwargs: dict

        :param timeout: Time, in seconds, to wait for a response to the command.
        :type timeout: float

        :return: Dictionary of values, dependent on the command response.
        """
        cmd_key = self.keyroot_cmd + "/%s" % xhost
        resp_key = self.keyroot_resp + "/%s" % xhost
        timestamp = time.time()
        sequence_id = str(int(timestamp * 1e6))
        command_json = self._format_command(
                           sequence_id,
                           timestamp,
                           block,
                           cmd,
                           kwargs = kwargs,
                       )
        if command_json is None:
            return False

        self._response_received = False
        self._response = None

        def response_callback(watchresponse):
            for event in watchresponse.events:
                self.log.debug("Got command response")
                try:
                    response_dict = json.loads(event.value.decode())
                except:
                    self.log.exception("Response JSON decode error")
                    continue
                self.log.debug("Response: %s" % response_dict)
                resp_id = response_dict.get("id", None)
                if resp_id == sequence_id:
                    self._response = response_dict
                    self._response_received = True
                else:
                    self.log.debug("Seq ID %s didn't match expected (%s)" % (resp_id, sequence_id))

        # Begin watching response channel and then send message
        watch_id = self.ec.add_watch_callback(resp_key, response_callback)
        # send command
        self.ec.put(cmd_key, command_json)
        starttime = time.time()
        while(True):
            if self._response_received:
                self.ec.cancel_watch(watch_id)
                status = self._response['val']['status']
                if status != 'normal':
                    self.log.info("Command status returned: '%s'" % status)
                return self._response['val']['response']
            if time.time() > starttime + timeout:
                self.ec.cancel_watch(watch_id)
                return None
            time.sleep(0.01)

    def _format_command(self, sequence_id, timestamp, block, cmd, kwargs={}):
        """
        Format a command to be sent via ETCD

        :param sequence_id: The ``id`` command field
        :type block: int

        :param timestamp: The ``timestamp`` command field
        :type timestamp: float

        :param block: The ``block`` command field
        :type block: str

        :param cmd: The ``cmd`` command field
        :type cmd: str

        :param kwargs: The ``kwargs`` command field
        :type kwargs: dict

        :return: JSON-encoded command string to be sent. Returns None if there
            is an enoding error.
        """
        command_dict = {
            "cmd": cmd,
            "val": {
                "block": block,
                "timestamp": timestamp,
                "kwargs": kwargs,
                },
            "id": sequence_id,
        }
        try:
            command_json = json.dumps(command_dict)
            return command_json
        except:
            self.log.exception("Failed to JSON-encode command")
            return

    def get_status(self, host, pipeline, block, inst_id, user_only=True):
        """
        Read a processing blocks status dictionary

        :param host: The hostname of the server running the DSP pipeline
            to be commanded
        :type host: string
        :param pipeline: The index of the pipeline on this server to be
            commanded
        :type pipeline: int
        :param block: The name of the processing block in this pipeline
            to be commanded
        :type block: string
        :param inst_id: The instance ID of the block of this type to be
            commanded
        :type inst_id: int
        :param user_only: If ``True`` read only statistics which are
            part of the block's user-level reporting capability. Otherwise,
            read all statistics, including those provided by the bifrost
            framework. In this case, user-level statistics are returned
            under the key name "stats"
        :type user_only: Bool

        :return: A dictionary of status values

        """

        key = self._get_mon_key(host, pipeline, block, inst_id)
        val, meta = self.ec.get(key)
        if val is None:
            self.log.warning("Etcd key %s returned no data" % key)
            return val
        val = json.loads(val)
        if user_only:
            return val.get("stats", {})
        else:
            return val


