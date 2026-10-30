import logging
import multiprocessing
from multiprocessing.dummy import connection
import time

import serial

class SerialProcess(multiprocessing.Process):
    def __init__(self, messageQ, commandQ, config) -> None:
        
        #run parent class initialiser
        super().__init__()

        self.logger = logging.getLogger('RFLinkGW.SerialProcessing')
        self.logger.info("Starting...")

        #multiprocessing.Process.__init__(self)

        self.__messageQ = messageQ
        self.__commandQ = commandQ
        #allows the process to use configf settings later
        self.config = config
        
        #OL updating to allow TCP connection
        self.connection_type = config.get('rflink_connection_type', 'serial').lower()
        
        if self.connection_type == 'tcp':
            host = config.get('rflink_tcp_host')
            port = config.get('rflink_tcp_port')
            if not host or not port:
                raise ValueError('TCP mode requires rflink_tcp_host and rflink_tcp_port')
            self.gatewayPort = 'socket://{}:{}'.format(host, port)
        elif self.connection_type == 'serial':
            self.gatewayPort = config['rflink_tty_device']
        else:
            raise ValueError('rflink_connection_type must be serial or tcp')
           
        self.baudrate = int(config.get('rflink_baudrate', 57600))
        self.timeout = float(config.get('rflink_timeout', 1))
        self.processing_exception = config.get('rflink_direct_output_params', [])
        self.processing_signed = config.get('rflink_signed_output_params', [])
        self.processing_wdir = config.get('rflink_wdir_output_params', [])
        self.sp = None   
     
    def close(self) -> None:
        #added check to makde sure the serial port is open before closing it
        if self.sp and self.sp.is_open:
            self.sp.close()
        self.logger.debug('RFLink connection closed')

    #Chooses how to create the connection and opens it
    def _open(self):
        #we already know is has to be either tcp or serial from the init, so we can just check for tcp and assume serial otherwise
        if self.connection_type == 'tcp':
            return serial.serial_for_url(self.gatewayPort, baudrate=self.baudrate,
                                         timeout=self.timeout)
        
        return serial.Serial(self.gatewayPort, baudrate=self.baudrate,
                             timeout=self.timeout)
    
    
    #Handles the connection to the serial port, and will retry if it fails
    def connect(self) -> None:
        while True:
            try:
                if self.sp and self.sp.is_open:
                    return
                self.logger.info('Connecting to RFLink via %s', self.gatewayPort)
                self.sp = self._open()
                self.logger.info('RFLink connection established')
                return
            except Exception as exc:
                self.logger.error('RFLink connection failed: %s', exc)
                #reconnect after a delay, default to 10 seconds if not specified in config
                time.sleep(float(self.config.get('rflink_reconnect_delay', 10)))

    #Doesnt need to relate to the instance, so can be a static method
    @staticmethod
    #Function that takes the data from the serial port and splits it into tokens, returning a list of parts and a dictionary of fields
    def _tokens(data):
        if isinstance(data, bytes):
            data = data.decode('ascii', errors='replace')
        parts = data.strip().rstrip(';').split(';')
        fields = {}
        for part in parts[1:]:
            if '=' in part:
                key, value = part.split('=', 1)
                fields[key.upper()] = value
        return parts, fields

    #for messages received - might rename as a bit misleading calling it output
    def prepare_output(self, data_in) -> list:
        parts, fields = self._tokens(data_in)
        if not parts or parts[0] != '20' or len(parts) < 3:
            return []
        family = parts[2]
        device_id = fields.get('ID') or fields.get('DEVICEID')
        switch = fields.get('SWITCH')
        if not device_id:
            return []
        out = []
        for key, raw in fields.items():
            if key in ('ID', 'DEVICEID'):
                continue
            value = raw
            try:
                if key in self.processing_exception:
                    pass
                elif key in self.processing_signed:
                    number = int(raw, 16)
                    value = -(number & 0x7fff) / 10.0 if number & 0x8000 else number / 10.0
                elif key in self.processing_wdir:
                    value = int(raw, 10) * 22.5
                elif key in ('CMD', 'SWITCH', 'BAT', 'MODEL', 'CHIME', 'UNIT'):
                    pass
                else:
                    value = int(raw, 16) / 10.0
            except (ValueError, TypeError):
                self.logger.debug('Keeping non-numeric RFLink value %s=%s', key, raw)
            topic = '{}/{}/READ/{}'.format(family, device_id, key)
            if key == 'CMD' and switch:
                topic = '{}/{}/{}/READ/{}'.format(family, device_id, switch, key)
            out.append({'method': 'publish', 'topic': topic, 'family': family,
                        'deviceId': device_id, 'switch': switch, 'param': key,
                        'payload': value, 'qos': 1, 'timestamp': time.time()})
        return out    
    
    def prepare_input(self, task):
        value = task['payload']
        
        if task.get('switch'):
            command = '10;{};{};{};CMD;{};\n'.format(
                task['family'], task['deviceId'], task['switch'], value)
        else:
            command = '10;{};{};{};{};\n'.format(
                task['family'], task['deviceId'], task['param'], value)
        self.logger.debug('Sending to RFLink: %s', command.strip())
        return command
    
    def run(self):
        self.logger.debug('SerialProcess.run started')
        self.connect()

        #Set timeout in case the connection fails for what ever reason, and we stop receiving data from 
        # the RFLink device. Default to 60 seconds if not specified in config
        idle_timeout_seconds = float(
            self.config.get('rflink_idle_timeout', 60)
        )
        last_message_time = time.monotonic()

        while True:
            try:
                try:
                    task = self.__commandQ.get_nowait()
                    self.sp.write(self.prepare_input(task).encode('ascii'))
                except Exception as exc:
                    if exc.__class__.__name__ != 'Empty':
                        self.logger.error('Send error: %s', exc)

                data = self.sp.readline()

                if data:
                    last_message_time = time.monotonic()
                    for task in self.prepare_output(data):
                        self.__messageQ.put(task)

                elif (
                    idle_timeout_seconds > 0
                    and time.monotonic() - last_message_time >= idle_timeout_seconds
                ):
                    self.logger.warning(
                        'No RFLink data for %.1f seconds; reconnecting',
                        idle_timeout_seconds
                    )
                    self.close()
                    self.connect()
                    last_message_time = time.monotonic()

            except Exception as exc:
                self.logger.error('RFLink receive error: %s', exc)
                try:
                    self.close()
                except Exception:
                    pass
                self.connect()
                last_message_time = time.monotonic()