# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import socket
from time import sleep

from deye_config import DeyeConfig


class DeyeConnector:
    def __init__(self, config: DeyeConfig) -> None:
        self.__log = logging.getLogger(DeyeConnector.__name__)
        self.config = config.logger
        self.__connected = False
        self.__connect()

    def __connect(self) -> bool:
        if self.__connected:  # Already connected to socket.
            return True
        self.__log.warning("Try to open socket on IP %s", self.config.ip_address)  # ToDO: INFO
        connect_error_log = logging.WARNING
        attempts = 3
        while attempts > 0:
            attempts = attempts - 1
            try:
                self.__socket = socket.create_connection((self.config.ip_address, self.config.port), timeout=10)
                self.__log.warning("Connected to socket on IP %s", self.config.ip_address)  # ToDo: INFO
                self.__connected = True
                return True
            except OSError as e:
                # Could not open socket on IP deye-solar.fritz.box: None: None: timed out: TimeoutError('timed out')
                # Could not open socket on IP deye-solar.fritz.box: Host is unreachable: 113: [Errno 113] Host is unreachable: OSError(113, 'Host is unreachable')
                self.__log.log(
                    connect_error_log,
                    "Could not open socket on IP %s: %s: %s: %s: %s",
                    self.config.ip_address,
                    e.strerror,
                    e.errno,
                    e,
                    repr(e),
                )
                connect_error_log = logging.WARNING  # ToDo: INFO/DEBUG
            except Exception:
                self.__log.exception("Unexpected connection error")
                return False
        return False

    def __send_frame(self, req_frame) -> bool:
        if self.__connect():
            try:
                self.__socket.sendall(req_frame)
                return True
            except OSError as e:
                # Connection error: [Errno 32] Broken pipe
                self.__log.warning("Failed to send request message: %s", e)

            except Exception:
                self.__log.exception("Unexpected connection error")
        return False

    def send_request(self, req_frame) -> bytes | None:
        self.__log.debug("Request frame: %s", req_frame.hex())
        if not self.__send_frame(req_frame):
            return  # Failed to connect and send request frame

        attempts = 5
        while attempts > 0:
            attempts = attempts - 1
            try:
                data = self.__socket.recv(1024)
                if data:
                    self.__log.debug("Response frame: %s", data.hex())
                    return data
                self.__log.warning("No data received")  # ToDo: Does a re-try really make sense?
            except socket.timeout:
                self.__log.debug("Connection response timeout")
                if attempts == 0:
                    self.__log.warning("Too many connection timeouts")
            except OSError as e:
                self.__connected = False  # Assume socked failed and is now disconnected
                # Connection error: Connection reset by peer: [Errno 104] Connection reset by peer
                self.__log.warning("Connection error: %s: %s", e.strerror, e)
                self.__log.warning(">> Request frame: %s", req_frame.hex())  # ToDo: remove/debug
                if not self.__send_frame(req_frame):  # Try to re-connect and re-send request frame
                    return
                # return
            except Exception:
                self.__log.exception("Unknown connection error")
                return

        return
