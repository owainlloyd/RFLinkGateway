import logging
import multiprocessing
import time
import paho.mqtt.client as mqtt

import json
import queue
import re
from ha_mqtt_discoverable import DeviceInfo, Settings
from ha_mqtt_discoverable.sensors import Sensor, SensorInfo
from pathlib import Path


def is_number(s) -> bool:
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False

class MQTTClient(multiprocessing.Process):
    def __init__(self, messageQ, commandQ, config) -> None:
        super().__init__()
        self.logger = logging.getLogger('RFLinkGW.MQTTClient')
        self.logger.info("Starting...")
        self.config=config
        self.__messageQ = messageQ
        self.__commandQ = commandQ

        # format values must be plain, json or ha (Home Assistant)
        self.mqttDataPrefix = self.config.get('mqtt_prefix', 'rflink')
        self.mqttDataFormat = self.config.get('mqtt_format', 'plain').lower()
        if self.mqttDataFormat not in ('plain', 'json', 'ha'):
            raise ValueError("mqtt.format must be 'plain', 'json', or 'ha'")


        self.client_connected = False
        self.connect_retry_counter = 0
    

        #HA autodiscovery settings       
        self._ha_mqtt_settings = None
        self._ha_sensors = {}
        self.ha_mapping = {}
        
        # if its ha then load the mapping file
        if self.mqttDataFormat == 'ha':
            self.ha_mapping = self._load_ha_mapping()

    def _load_ha_mapping(self) -> dict:
        mapping_file = Path(__file__).resolve().parent / 'ha_mapping.json'

        try:
            with mapping_file.open(encoding='utf-8') as mapping_stream:
                mapping = json.load(mapping_stream)
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError('Could not load HA mapping file {}: {}'.format(
                mapping_file, exc
            )) from exc
        if not isinstance(mapping, dict):
            raise ValueError('HA mapping file must contain a JSON object')
        self.logger.info('Loaded HA device mappings from %s', mapping_file)
        return mapping

    
    def connect (self,config) -> None:
        try:
            self._mqttConn.connect(config['mqtt_host'], port=config['mqtt_port'], keepalive=120)
        except Exception as e:
            self.logger.error("problem with connect: %s" % e)
    
    def close(self) -> None:
        self.logger.info("Closing connection")
        self._mqttConn.disconnect()

    def _on_connect(self, client, userdata, flags, rc) -> None:
        #if return code is 0, then we are connected
        if rc == 0:
            self.client_connected = True
            self.connect_retry_counter = 0
            self.logger.info('Client connected')

            # Subscribe to the topic for WRITE commands
            client.subscribe('%s/+/+/WRITE/+' % self.mqttDataPrefix)
            
            if self.mqttDataFormat == 'ha' and self._ha_mqtt_settings is None:
                self._ha_mqtt_settings = Settings.MQTT(
                    client=client,
                    discovery_prefix=self.config.get('ha_discovery_prefix', 'homeassistant'),
                    state_prefix=self.config.get('mqtt_prefix', 'hmd'),
                )
        else:
            self.client_connected = False
            self.logger.error('MQTT connection rejected: %s', rc)


    def _on_disconnect(self, client, userdata, rc) -> None:
        if rc != 0:
            self.logger.error("Unexpected disconnection.")
            self.client_connected = False
            self.connect(self.config)

    def _on_publish(self, client, userdata, mid) -> None:
        self.logger.debug("Message " + str(mid) + " published.")

    def _on_message(self, client, userdata, message) -> None:
        self.logger.debug("Message received: %s" % (message))

        data = message.topic.replace(self.config['mqtt_prefix'] + "/", "").split("/")
        data_out = {
            'method': 'subscribe',
            'topic': message.topic,
            'family': data[0],
            'deviceId': data[1],
            'param': data[3],
            'payload': message.payload.decode('ascii'),
            'qos': 1
        }
        self.__commandQ.put(data_out)


    # Helper function to create a slug (simplfied text label) from a string
    @staticmethod
    def _slug(value) -> str:
        return re.sub(r'[^a-z0-9_]+', '_', str(value).lower()).strip('_')

    def _get_ha_sensor(self, task):
        """Find or create the sensor configured for this RFLink update."""
        family = str(task.get('family', ''))
        device_id = str(task.get('deviceId', ''))
        parameter = str(task.get('param', '')).upper()

        family_config = self.ha_mapping.get('families', {}).get(family)
        if not family_config:
            return None

        device_type = family_config.get('device_type')
        type_config = self.ha_mapping.get('device_types', {}).get(device_type, {})
        parameter_config = type_config.get('parameters', {}).get(parameter)
        if not parameter_config:
            return None

        cache_key = (family, device_id, parameter)
        if cache_key in self._ha_sensors:
            return self._ha_sensors[cache_key]

        device_name = '{} {}'.format(family, device_id)
        device_options = {
            'name': device_name,
            'identifiers': 'rflink_{}_{}'.format(
                self._slug(family), self._slug(device_id)
            ),
        }
        for key in ('manufacturer', 'model', 'sw_version'):
            if family_config.get(key) is not None:
                device_options[key] = family_config[key]
        device_info = DeviceInfo(**device_options)

        sensor_options = {
            'name': self._slug('{}_{}_{}'.format(family, device_id, parameter)),
            'display_name': parameter_config.get(
                'name', '{} {}'.format(device_name, parameter)
            ),
            'unique_id': 'rflink_{}_{}_{}'.format(
                self._slug(family), self._slug(device_id), self._slug(parameter)
            ),
            'device': device_info,
        }
        for key in ('device_class', 'state_class', 'icon'):
            if parameter_config.get(key) is not None:
                sensor_options[key] = parameter_config[key]

        unit = parameter_config.get(
            'unit_of_measurement', parameter_config.get('unit')
        )
        if unit is not None:
            sensor_options['unit_of_measurement'] = unit

        sensor = Sensor(Settings(
            mqtt=self._ha_mqtt_settings,
            entity=SensorInfo(**sensor_options),
        ))
        self._ha_sensors[cache_key] = sensor
        self.logger.info('Created HA sensor for %s/%s/%s', family, device_id, parameter)
        return sensor

    def _publish_ha_state(self, task) -> bool:
        sensor = self._get_ha_sensor(task)
        if sensor is None:
            return False
        sensor.set_state(task['payload'])
        return True


    def publish(self, task) -> None:
        if self.mqttDataFormat == 'ha':
            # HA mode uses the discovery library's state topics only.
            if self._ha_mqtt_settings is None:
                self.logger.debug('MQTT not ready for HA publish; requeueing task')
                self.__messageQ.put(task)
                return
            try:
                if not self._publish_ha_state(task):
                    self.logger.warning(
                        'No HA mapping for %s/%s/%s; message not published',
                        task.get('family'), task.get('deviceId'), task.get('param')
                    )
            except Exception:
                self.logger.exception('Home Assistant sensor update failed')
            return

        # Legacy mode publishes to the existing RFLink topic only.
        topic = '{}/{}'.format(self.mqttDataPrefix, task['topic'])
        payload = task['payload']
        
        if self.mqttDataFormat == 'json':
            payload = json.dumps({'value': payload})

        try:
            result = self._mqttConn.publish(
                topic, payload=str(payload), qos=task.get('qos', 0)
            )
            self.logger.debug(
                'Sending message %s :%s, result:%s', result.mid, task, result.rc
            )
            if result.rc != 0:
                raise RuntimeError('MQTT publish failed with code {}'.format(result.rc))
        except Exception:
            self.logger.exception('Publish problem')
            self.__messageQ.put(task)


    def run(self):
        #OL Added
        self._mqttConn = mqtt.Client(client_id='RFLinkGateway')      
        self._mqttConn.username_pw_set(self.config['mqtt_user'], self.config['mqtt_password'])
        self._mqttConn.on_disconnect = self._on_disconnect
        self._mqttConn.on_publish = self._on_publish
        self._mqttConn.on_message = self._on_message
        self._mqttConn.on_connect = self._on_connect
        self.connect(self.config)

        while True:
            if self.client_connected == False:
                #TODO Add reconnection limit
                time.sleep (1+2*self.connect_retry_counter)
                self.logger.error('Reconnecting, try:%s' % (self.connect_retry_counter+1))
                self.connect(self.config)
                self.connect_retry_counter += 1    
            if not self.__messageQ.empty():
                task = self.__messageQ.get()
                if task['method'] == 'publish':
                    self.publish(task)
            else:
                time.sleep(0.1)
            self._mqttConn.loop()
