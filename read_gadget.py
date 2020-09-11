import yt

ds = yt.load_sample("snapshot_033")

reg = ds.all_data()

class MockSelector:
    is_all_data = True

class MockChunkObject:
    def __init__(self, data_file):
        self.data_files = [data_file]

class MockChunk:
    def __init__(self, data_file):
        self.objs = [MockChunkObject(data_file)]

ptf = {'PartType0': ['Coordinates']}

chunks = [MockChunk(data_file) for data_file in ds.index.data_files]
selector= MockSelector()

my_gen = ds.index.io._read_particle_fields(chunks, ptf, selector)
my_result = [_ for _ in my_gen]
