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

from deye_config import DeyeConfig


class DeyeConnector:
    def __init__(self, config: DeyeConfig) -> None:
        self.__log = logging.getLogger(DeyeConnector.__name__)
        self.config = config.logger
        self.__missed_requests = 0

    def send_request(self, req_frame) -> bytes | None:
        try:
            with socket.create_connection(
                (self.config.ip_address, self.config.port), timeout=self.config.timeout
            ) as client_socket:
                if self.__missed_requests:
                    self.__log.log(logging.WARNING if self.__missed_requests > 5 else logging.INFO,
                        "Re-connected to socket on IP %s, missed %s requests",
                        self.config.ip_address,
                        self.__missed_requests,
                    )
                    self.__missed_requests = 0
                self.__log.debug("Request frame: %s", req_frame.hex())
                client_socket.sendall(req_frame)
                data = client_socket.recv(1024)
                if data:
                    self.__log.debug("Response frame: %s", data.hex())
                    return data
                self.__log.warning("No data received")
        except socket.timeout:
            self.__log.warning("Connection response timeout after %s seconds", self.config.timeout)
        except OSError as e:
            if self.__missed_requests:
                self.__log.debug("Connection error on IP %s: %s", self.config.ip_address, e)
            else:
                self.__log.warning("Connection error on IP %s: %s", self.config.ip_address, e)
            self.__missed_requests += 1
        except Exception:
            self.__log.exception("Unknown connection error")
            self.__missed_requests += 1

        return
