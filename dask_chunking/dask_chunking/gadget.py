# example module containing functions for using dask.delayed to lazy load gadget chunks 
# and then compute stats across gadget chunks with dask.delayed. 
#
# main method to call: selector_stats()
# 
# some "features": 
# * runs in parallel if a dask Client is running: reading and local by-chunk stats will 
#   be spread across the Client. Global cross-chunk stats are calculated by aggregation 
#   of local by-chunk stats.  
# * works with selection objects
# * ONLY WORKS FOR ONE FIELD RIGHT NOW 
#
# example usage: 
#     import yt 
#     from dask_chunking import gadget as ga 
#     ds = yt.load_sample("snapshot_033")
#     ptf = {'PartType0': 'Mass'}    
#     sp = ds.sphere(ds.domain_center,(2,'code_length')) 
#     glob_stats, chunk_stats = ga.selector_stats(ds,ptf,sp.selector)
# 
import yt
import h5py
from dask import dataframe as df, array as da, delayed, compute
import numpy as np 
import dask

def load_single_chunk(self,data_file,ptf,selector):
    si, ei = data_file.start, data_file.end
    f = h5py.File(data_file.filename, mode="r")
    chunk_data = []
    for ptype, field_list in sorted(ptf.items()):
        if data_file.total_particles[ptype] == 0:
            continue
        g = f[f"/{ptype}"]
        if getattr(selector, "is_all_data", False):
            mask = slice(None, None, None)
            mask_sum = data_file.total_particles[ptype]
            hsmls = None
        else:
            coords = g["Coordinates"][si:ei].astype("float64")
            if ptype == "PartType0":
                hsmls = self._get_smoothing_length(
                    data_file, g["Coordinates"].dtype, g["Coordinates"].shape
                ).astype("float64")
            else:
                hsmls = 0.0
            mask = selector.select_points(
                coords[:, 0], coords[:, 1], coords[:, 2], hsmls
            )
            if mask is not None:
                mask_sum = mask.sum()
            del coords
        if mask is None:
            continue
        for field in field_list:
            if field in ("Mass", "Masses") and ptype not in self.var_mass:
                data = np.empty(mask_sum, dtype="float64")
                ind = self._known_ptypes.index(ptype)
                data[:] = self.ds["Massarr"][ind]
            elif field in self._element_names:
                rfield = "ElementAbundance/" + field
                data = g[rfield][si:ei][mask, ...]
            elif field.startswith("Metallicity_"):
                col = int(field.rsplit("_", 1)[-1])
                data = g["Metallicity"][si:ei, col][mask]
            elif field.startswith("GFM_Metals_"):
                col = int(field.rsplit("_", 1)[-1])
                data = g["GFM_Metals"][si:ei, col][mask]
            elif field.startswith("Chemistry_"):
                col = int(field.rsplit("_", 1)[-1])
                data = g["ChemistryAbundances"][si:ei, col][mask]
            elif field == "smoothing_length":
                # This is for frontends which do not store
                # the smoothing length on-disk, so we do not
                # attempt to read them, but instead assume
                # that they are calculated in _get_smoothing_length.
                if hsmls is None:
                    hsmls = self._get_smoothing_length(
                        data_file,
                        g["Coordinates"].dtype,
                        g["Coordinates"].shape,
                    ).astype("float64")
                data = hsmls[mask]
            else:
                data = g[field][si:ei][mask, ...]

            if data.size > 0:
                # NOTE: we're actually SUBCHUNKING here! 
                subchunk_size=100000
                if data.ndim > 1:
                    subchunk_shape = (subchunk_size,1)  # dont chunk up multidim arrays like Coordinates
                else:
                    subchunk_shape = (subchunk_size)  

                chunk_data.append([(ptype, field), da.from_array(data,chunks=subchunk_shape)])
    f.close()
    return chunk_data 
    
def read_particle_fields_delayed(self,chunks, ptf, selector):
    # returns a list of dask-delayed chunks, agnostic to chunk sizes 
    
    # let's still loop over the chunks 
    data_files = set([])
    for chunk in chunks:
        for obj in chunk.objs:
            data_files.update(obj.data_files)
            
    # and we still loop over each base chunk  
    all_chunks=[]
    for data_file in sorted(data_files, key=lambda x: (x.filename, x.start)):
        ch = delayed(load_single_chunk(self,data_file,ptf,selector))            
        all_chunks.append(ch)
    
    return all_chunks 
    
class MockSelector:
    is_all_data = True

class MockChunkObject:
    def __init__(self, data_file):
        self.data_files = [data_file]

class MockChunk:
    def __init__(self, data_file):
        self.objs = [MockChunkObject(data_file)]

@dask.delayed 
def npmeth(chunk,meths=['min']): 
    # returns attribute values like min(), max(), size. 
    results = []   
    if len(chunk)>0:         
        x = chunk[0][1]
        for meth in meths: 
            if hasattr(x,meth):
                this_meth = getattr(x,meth)
                if callable(this_meth):
                    results.append(this_meth())
                else:
                    results.append(this_meth) 
    return dict(zip(meths,results)) 
    
def selector_stats(ds,ptf,selector=None):
    # for a given dataset and selctor, calculate some local and global stats. 
    # "local" = each chunk, "global" = across chunks 
    
    if selector is None: 
        selector = MockSelector() 
        
    # assemble data_file "mock" chunks 
    chunks = [MockChunk(data_file) for data_file in ds.index.data_files]    
    
    # read chunks (delayed)
    print("building delayed read objects ")
    my_gen_delayed = read_particle_fields_delayed(ds.index.io, chunks, ptf, selector)

    # calculate chunk stats (delayed)
    print("building delayed stats")
    stat_meths = ['min','max','sum','size','std']
    stats = [npmeth(chunk,stat_meths) for chunk in my_gen_delayed]

    # actually read and compute the stats -- chunk data will not be stored, only the stats 
    print("computing local stats")
    chunk_stats = dask.compute(*stats)
    
    # calculate some global stats 
    print("computing global stats")
    minvals = []
    maxvals = []
    total_sum = 0
    total_size = 0
    for chunk in chunk_stats:        
        if 'sum' in chunk.keys() and 'size' in chunk.keys():
            total_sum += chunk['sum']
            total_size += chunk['size']            
        if 'min' in chunk.keys():
            minvals.append(chunk['min'])            
        if 'max' in chunk.keys():
            maxvals.append(chunk['max'])
            
    global_stats = {'min': np.min(minvals), 
                    'max': np.min(maxvals),
                    'mean': total_sum / total_size,
                    'size':total_size}
                    
    return global_stats, chunk_stats 
