import pickle


class StoreConfig:
    def __init__(self, data):
        self.data = data


doc = {
    "credit": 100,
    "list": list(range(10))
}

store_config = StoreConfig(doc)
with open("store.config", "wb") as f:  # Pickling
    pickle.dump(store_config, f)

with open("store.config", "rb") as f:   # Unpickling
    read_config = pickle.load(f)

print(read_config.data)
