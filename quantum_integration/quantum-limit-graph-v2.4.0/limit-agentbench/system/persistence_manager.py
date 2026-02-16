import os
import json
import torch


class PersistenceManager:

    @staticmethod
    def atomic_json_save(data, path):
        temp_path = path + ".tmp"
        with open(temp_path, "w") as f:
            json.dump(data, f)
        os.replace(temp_path, path)

    @staticmethod
    def atomic_model_save(model, path):
        temp_path = path + ".tmp"
        torch.save(model.state_dict(), temp_path)
        os.replace(temp_path, path)

    @staticmethod
    def load_model(model, path):
        if os.path.exists(path):
            model.load_state_dict(torch.load(path))
