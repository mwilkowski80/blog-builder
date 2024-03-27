import hashlib
import json
import logging
import os.path
import shutil
import string
import tempfile
from dataclasses import dataclass
from io import StringIO
from pathlib import Path
from typing import Callable, Union, Optional
from datetime import datetime
from structlog.stdlib import get_logger as get_raw_logger

Payload = Union[str, dict, list]
logger = get_raw_logger(os.path.basename(__file__))


@dataclass
class Metadata:
    key: str
    hash_key: str
    created_at: datetime

    @classmethod
    def from_dict(cls, dict_: dict) -> 'Metadata':
        return Metadata(
            key=dict_['key'],
            hash_key=dict_['hash-key'],
            created_at=datetime.fromisoformat(dict_['created_at']))

    def to_dict(self) -> dict:
        return {'key': self.key, 'hash-key': self.hash_key, 'created_at': self.created_at.isoformat()}


@dataclass
class Cacheable:
    metadata: Metadata
    payload: Payload

    @classmethod
    def from_dict(cls, dict_: dict) -> 'Cacheable':
        return Cacheable(
            metadata=Metadata.from_dict(dict_['metadata']),
            payload=dict_['payload'])

    def to_dict(self) -> dict:
        return {'metadata': self.metadata.to_dict(), 'payload': self.payload}


class Cache:
    def __init__(self):
        self._log = logging.getLogger(__package__ + '.' + __name__ + '.' + Cache.__name__)

    @staticmethod
    def hash_key(key: str) -> str:
        return hashlib.md5(key.encode('utf-8')).hexdigest()

    def get(self, key: str, supplier: Callable[[], Payload], prefix_key='') -> Cacheable:
        hash_key = prefix_key + Cache.hash_key(key)
        cacheable = self._get_if_exists(hash_key)
        if cacheable:
            self._log.debug(f'Found object in cache: "{hash_key}"')
            logger.info(
                "Cached response used",
                hash_key=hash_key,
            )
        else:
            self._log.debug(f'Object "{hash_key}" not found in cache')
            logger.info(
                "Cached response not found",
                hash_key=hash_key,
            )
            payload = supplier()
            cacheable = Cacheable(
                payload=payload,
                metadata=Metadata(key=key, hash_key=hash_key, created_at=datetime.utcnow()))
            self._persist(cacheable)
        return cacheable

    def _get_if_exists(self, hash_key: str) -> Optional[Cacheable]:
        raise NotImplementedError()

    def _persist(self, cacheable: Cacheable) -> None:
        raise NotImplementedError()


LEGAL_CHARS = string.ascii_letters + '0123456789_-'
ENCODE_PREFIX_CHAR = '+'


class NoOpCache(Cache):
    def _get_if_exists(self, hash_key: str) -> Optional[Cacheable]:
        return None

    def _persist(self, cacheable: Cacheable) -> None:
        pass


class FilesystemCache(Cache):
    def __init__(self, dir_: Path) -> None:
        super(FilesystemCache, self).__init__()
        self._dir = dir_

    @staticmethod
    def _encode_name(input_str: str) -> str:
        if not input_str or len(input_str.strip()) == 0:
            raise ValueError('You cannot encode empty name as a local filename')
        input_str.encode('ascii')

        output = StringIO()
        for c in input_str:
            if c in LEGAL_CHARS:
                output.write(c)
            else:
                output.write(ENCODE_PREFIX_CHAR)
                output.write(hex(ord(c))[2:])
        return output.getvalue()

    def _persist(self, cacheable: Cacheable) -> None:
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as ntf:
            try:
                obj_filepath = self._resolve_path(cacheable.metadata.hash_key)
                json.dump(cacheable.to_dict(), ntf)
                ntf.flush()
                shutil.move(ntf.name, obj_filepath)
            except:
                os.remove(ntf.name)
                raise

    def _resolve_path(self, key: str) -> Path:
        encoded_name = FilesystemCache._encode_name(key) + '.json'
        return self._dir / encoded_name

    def _ensure_dir_exists(self) -> None:
        if not os.path.isdir(self._dir):
            raise FileNotFoundError(f'I could not find a directory for a local cache: {self._dir}')

    def _get_if_exists(self, hash_key: str) -> Optional[Cacheable]:
        self._ensure_dir_exists()
        obj_filepath = self._resolve_path(hash_key)
        if os.path.exists(obj_filepath):
            with open(obj_filepath) as f:
                return Cacheable.from_dict(json.load(f))
