"""
    Author: noahcardoza@gmail.com
    Licence: MIT
"""

import json
import threading
from collections.abc import MutableMapping
from os import path


class JSONDatabase(MutableMapping):
    """
    A thread-safe data store which serializes data in JSON

    It's best to use the `with` statement when working with
    data to make use the data doens't get tampered with by
    another thread, but make sure you don't have any long
    blocking calls or it could hang other threads for a while
    """

    def __init__(self, location: str, default: dict = {}, overwrite=False):
        self.location = location
        self.store = default
        self.lock = threading.Lock()
        if overwrite or not path.exists(location):
            self.dump()
        else:
            self.load()

    def overwrite(self, store):
        self.store = store
        self.dump()

    def dump(self):
        with self.lock, open(self.location, 'w') as f:
            json.dump(self.store, f)

    def load(self):
        with self.lock, open(self.location, 'r') as f:
            self.store = json.load(f)

    def __enter__(self):
        self.lock.acquire()
        return

    def __exit__(self, type, value, traceback):
        self.lock.release()
        self.dump()

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value

    def __delitem__(self, key):
        del self.store[key]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __repr__(self):
        return f'JSONDatabase<{self.store.__repr__()}>'

    def __str__(self):
        return self.store.__str__()


if __name__ == '__main__':
    db = JSONDatabase('harvester.db.json', {
        'list': []
    }, True)

    print(db)

    assert len(json.load(open('harvester.db.json', 'r'))['list']) == 0

    with db:
        db['list'].append(42)

    print(db)

    assert json.load(open('harvester.db.json', 'r'))['list'][0] == 42

    db['dict'] = {'answer': 42}
    db.dump()

    print(db)

    assert json.load(open('harvester.db.json', 'r'))['dict']['answer'] == 42

    db.overwrite({})

    print(db)

    assert len(json.load(open('harvester.db.json', 'r'))) == 0
