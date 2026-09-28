

class BaseDataLoader():

    def load_series(self, id):
        raise NotImplementedError()

    def save_series(self, id, time_arr, data_arr):
        raise NotImplementedError()
    
    def delete(self, id):
        raise NotImplementedError()