#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import typing
import uuid

from arch.common import EngineType, engine_utils
from arch.abc import CSessionABC, CTableABC
from arch.common import log
from arch.computing import ComputingEngine
from arch.session._parties import PartiesInfo
from arch.abc import CSessionABC, CTableABC, StorageSessionABC, StorageTableABC, StorageTableMetaABC

LOGGER = log.getLogger()


class Session(object):
    __GLOBAL_SESSION = None

    @classmethod
    def get_global(cls):
        return cls.__GLOBAL_SESSION

    @classmethod
    def _as_global(cls, sess):
        cls.__GLOBAL_SESSION = sess

    def as_global(self):
        self._as_global(self)
        return self

    def __init__(self, session_id: str = None, options=None):
        if options is None:
            options = {}
        engines = engine_utils.get_engines()
        LOGGER.info(f"using engines: {engines}")
        computing_type = engines.get(EngineType.COMPUTING, None)
        if computing_type is None:
            raise RuntimeError(f"must set default engines on conf/service_conf.yaml")

        self._computing_type = engines.get(EngineType.COMPUTING, None)
        self._storage_engine = engines.get(EngineType.STORAGE, None)
        self._computing_session: typing.Optional[CSessionABC] = None
        self._storage_session: typing.Dict[StorageSessionABC] = {}
        self._parties_info: typing.Optional[PartiesInfo] = None
        self._session_id = str(uuid.uuid1()) if not session_id else session_id
        self._logger = LOGGER if options.get("logger", None) is None else options.get("logger", None)

        self._logger.info(f"create manager session {self._session_id}")

    @property
    def session_id(self) -> str:
        return self._session_id

    def _open(self):
        return self

    def _close(self):
        self.destroy_all_sessions()

    def __enter__(self):
        return self._open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb:
            self._logger.exception("", exc_info=(exc_type, exc_val, exc_tb))
        return self._close()

    def init_computing(self,
                       computing_session_id: str = None,
                       **kwargs):
        computing_session_id = f"{self._session_id}_computing_{uuid.uuid1()}" if not computing_session_id else computing_session_id
        if self.is_computing_valid:
            raise RuntimeError(f"computing session already valid")

        if self._computing_type == ComputingEngine.STANDALONE:
            from arch.computing.standalone import CSession

            self._computing_session = CSession(session_id=computing_session_id)
            self._computing_type = ComputingEngine.STANDALONE
            return self

        if self._computing_type == ComputingEngine.SPARK:
            from arch.computing.spark import CSession

            self._computing_session = CSession(session_id=computing_session_id)
            self._computing_type = ComputingEngine.SPARK
            return self

        raise RuntimeError(f"{self._computing_type} not supported")

    def _get_or_create_storage(self,
                               storage_session_id=None,
                               storage_engine=None,
                               record: bool = True,
                               **kwargs) -> StorageSessionABC:
        storage_session_id = f"{self._session_id}_storage_{uuid.uuid1()}" if not storage_session_id else storage_session_id

        if storage_session_id in self._storage_session:
            return self._storage_session[storage_session_id]
        else:
            if storage_engine is None:
                storage_engine = self._storage_engine

        for session in self._storage_session.values():
            if storage_engine == session.engine:
                return session

        if record:
            self.save_record(engine_type=EngineType.STORAGE,
                             engine_name=storage_engine,
                             engine_session_id=storage_session_id)

        elif storage_engine == StorageEngine.STANDALONE:
            from arch.storage.standalone import StorageSession
            storage_session = StorageSession(session_id=storage_session_id, options=kwargs.get("options", {}))

        elif storage_engine == StorageEngine.MYSQL:
            from arch.storage.mysql import StorageSession
            storage_session = StorageSession(session_id=storage_session_id, options=kwargs.get("options", {}))

        elif storage_engine == StorageEngine.HDFS:
            from arch.storage.hdfs import StorageSession
            storage_session = StorageSession(session_id=storage_session_id, options=kwargs.get("options", {}))

        elif storage_engine == StorageEngine.HIVE:
            from arch.storage.hive import StorageSession
            storage_session = StorageSession(session_id=storage_session_id, options=kwargs.get("options", {}))

        elif storage_engine == StorageEngine.LINKIS_HIVE:
            from arch.storage.linkis_hive import StorageSession
            storage_session = StorageSession(session_id=storage_session_id, options=kwargs.get("options", {}))

        elif storage_engine == StorageEngine.PATH:
            from arch.storage.path import StorageSession
            storage_session = StorageSession(session_id=storage_session_id, options=kwargs.get("options", {}))

        elif storage_engine == StorageEngine.LOCALFS:
            from arch.storage.localfs import StorageSession
            storage_session = StorageSession(session_id=storage_session_id, options=kwargs.get("options", {}))

        else:
            raise NotImplementedError(f"can not be initialized with storage engine: {storage_engine}")

        self._storage_session[storage_session_id] = storage_session

        return storage_session

    def get_table(self, name, namespace) -> typing.Union[StorageTableABC, None]:
        meta = Session.get_table_meta(name=name, namespace=namespace)
        if meta is None:
            return None
        engine = meta.get_engine()
        storage_session = self._get_or_create_storage(storage_engine=engine)
        table = storage_session.get_table(name=name, namespace=namespace)
        return table

    @classmethod
    def get_table_meta(cls, name, namespace) -> typing.Union[StorageTableMetaABC, None]:
        meta = StorageSessionBase.get_table_meta(name=name, namespace=namespace)
        return meta

    @classmethod
    def persistent(cls, computing_table: CTableABC, namespace, name, schema=None, part_of_data=None,
                   engine=None, engine_address=None, store_type=None, token: typing.Dict = None) -> StorageTableMetaABC:
        return StorageSessionBase.persistent(computing_table=computing_table,
                                             namespace=namespace,
                                             name=name,
                                             schema=schema,
                                             part_of_data=part_of_data,
                                             engine=engine,
                                             engine_address=engine_address,
                                             store_type=store_type,
                                             token=token)

    @property
    def computing(self) -> CSessionABC:
        return self._computing_session

    def storage(self, **kwargs):
        return self._get_or_create_storage(**kwargs)

    @property
    def parties(self):
        return self._parties_info

    @property
    def is_computing_valid(self):
        return self._computing_session is not None

    @property
    def is_federation_valid(self):
        return self._federation_session is not None

    def _init_computing_if_not_valid(self, computing_session_id):
        if not self.is_computing_valid:
            self.init_computing(computing_session_id=computing_session_id, record=False)
            return True
        elif self._computing_session.session_id != computing_session_id:
            self._logger.warning(
                f"manager session had computing session {self._computing_session.session_id} different with query from db session {computing_session_id}")
            return False
        else:
            # already exists
            return True

    def destroy_all_sessions(self, **kwargs):
        self._logger.info(f"start destroy manager session {self._session_id} all sessions")
        self.get_session_from_record(**kwargs)
        self.destroy_storage_session()
        self.destroy_computing_session()
        self._logger.info(f"finish destroy manager session {self._session_id} all sessions")

    def destroy_computing_session(self):
        if self.is_computing_valid:
            try:
                self._logger.info(f"try to destroy computing session {self._computing_session.session_id}")
                try:
                    self._computing_session.stop()
                except:
                    self._computing_session.kill()
                self._logger.info(f"destroy computing session {self._computing_session.session_id} successfully")
            except Exception as e:
                self._logger.info(f"destroy computing session {self._computing_session.session_id} failed", e)
            self.delete_session_record(engine_session_id=self._computing_session.session_id)

    def destroy_storage_session(self):
        for session_id, session in self._storage_session.items():
            try:
                self._logger.info(f"try to destroy storage session {session_id}")
                session.destroy()
                self._logger.info(f"destroy storage session {session_id} successfully")
            except Exception as e:
                self._logger.exception(f"destroy storage session {session_id} failed", e)
            self.delete_session_record(engine_session_id=session_id)


def get_session() -> Session:
    return Session.get_global()


def get_parties() -> PartiesInfo:
    return get_session().parties


def get_computing_session() -> CSessionABC:
    return get_session().computing


# noinspection PyPep8Naming
class computing_session(object):
    @staticmethod
    def init(session_id, options=None):
        Session(options=options).as_global().init_computing(session_id)

    @staticmethod
    def parallelize(data: typing.Iterable, partition: int, include_key: bool, **kwargs) -> CTableABC:
        return get_computing_session().parallelize(data, partition=partition, include_key=include_key, **kwargs)

    @staticmethod
    def stop():
        get_computing_session().stop()
