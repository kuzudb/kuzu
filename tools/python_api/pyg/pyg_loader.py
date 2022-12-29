import time
import numpy as np

from data_generator import PyGDataGenerator

from torch_geometric.loader import RandomNodeLoader

batch_size = 100
shuffle = False
num_workers = 8
num_epoch = 1

def load(data):
    num_parts = data.num_nodes / batch_size
    print(num_parts)
    loader = RandomNodeLoader(data=data, num_parts=num_parts, shuffle=shuffle, num_workers=num_workers)
    start_time = time.time()
    counter = 0
    for batch in loader:
        counter += batch_size
        if counter % 10000 == 0:
            print(counter)
    end_time = time.time()
    return end_time - start_time

if __name__ == '__main__':
    generator = PyGDataGenerator()
    data = generator.generate()
    print(data)
    
    time_array = np.empty(num_epoch)
    for epoch in range(num_epoch):
        time_array[epoch] = load(data)
        print(str.format('Epoch: {}', epoch))
    print(str.format('Min: {:.2f}, Max: {:.2f}, Mean: {:.2f}, Std: {:.2f}', time_array.min(), time_array.max(), time_array.mean(), time_array.std()))


