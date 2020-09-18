# read in chunks into pre-allocated dask arrays THEN filter 
import h5py
from dask import dataframe as df, array as da, delayed, compute
import numpy as np 
import dask
import yt

class MockSelector:
    is_all_data = True

class MockChunkObject:
    def __init__(self, data_file):
        self.data_files = [data_file]

class MockChunk:
    def __init__(self, data_file):
        self.objs = [MockChunkObject(data_file)]
        
class delayed_gadget(object):    
    def __init__(self,ds, ptf = None, subchunk_size = 100000):
        
        # references that we need to read in gadget data 
        self.var_mass = ds.index.io.var_mass
        self._element_names = ds.index.io._element_names
        
        # initialize the chunks 
        self.chunks = [MockChunk(data_file) for data_file in ds.index.data_files]
        self.data_files = self.return_data_files()
        self.subchunk_size = subchunk_size
        
        # initialize the delayed on full dataset 
        if ptf is None: 
            self.delayed_chunks = []
            self.ptf = None 
        else:
            self.stage_chunks(ptf) 
                
        self.masks=[]
        
    def return_data_files(self):    
        data_files = set([])
        for chunk in self.chunks:
            for obj in chunk.objs:
                data_files.update(obj.data_files)
        return data_files
    
    def stage_chunks(self,ptf):
        self.ptf = ptf 
        self.delayed_chunks = []
        for df in self.data_files:
            # dask_delayed will need to serialize all objects passed -- can't handle df object, so 
            # let's just pull out what we need for this chunk:
            df_dict = {key:getattr(df,key) for key in ['filename','start','end','total_particles']}
            df_dict['var_mass']=self.var_mass
            df_dict['_element_names']=self._element_names                        
            self.delayed_chunks.append(delayed_chunk_read(self.ptf,df_dict,self.subchunk_size))
            
    def set_chunk_masks(self,selector):
        # ideally we'd assemlbe the masks based on the delayed chunk and a selector
        # and then we could apply any filtering on top of the delayed read, 
        # but this fails because dask cant serialize the selector object 
        self.masks=[]
        for chunk in self.delayed_chunks:
            self.masks.append(get_chunk_masks(self.ptf,chunk,selector))
    
# pickle-serialization safe object to give to dask delayed
@dask.delayed
def delayed_chunk_read(ptf,df_dict,subchunk_size):
    return chunk_reader(ptf,df_dict,subchunk_size).read()
    
class chunk_reader(object):
    def __init__(self,ptf,df_dict,subchunk_size,selector=None):           
        for key,val in df_dict.items():
            setattr(self,key,val)            
        self.ptf = ptf 
        self.subchunk_size = subchunk_size 
        self.f = None # hdf file handle 
        self.selector = selector 
        return   
    
    def read(self):        
        self.f = h5py.File(self.filename, mode="r")
        coords, hsmls = self.load_chunk_coords()        
        data = self.read_chunk_fields(hsmls)                              
        total_particles={}
        for ptype, field_list in sorted(self.ptf.items()):
            total_particles[ptype] = self.total_particles[ptype]
        self.f.close()
        
        return coords, data , total_particles, hsmls
        
    def load_chunk_coords(self):        
        # coords are stored by ptype, also store smoothing length 
        coords = {}
        hsmls = {}
        si = self.start 
        ei = self.end 
        for ptype, field_list in sorted(self.ptf.items()):
            g = self.f[f"{ptype}"]
                        
            if self.subchunk_size is None:
                coords[ptype] = g["Coordinates"][si:ei].astype("float64")
            else:
                coords[ptype] = da.from_array(g["Coordinates"][si:ei].astype("float64"), chunks=(self.subchunk_size,1))
            
            if ptype == "PartType0":
                # gadget hdf smoothing length as in yt/frontends/gadget/io.py
                ds = g["SmoothingLength"][si:ei, ...]
                dt = ds.dtype.newbyteorder("N")  # Native
                if dt < np.float64:
                    dt = np.float64
                hsmls[ptype] = np.empty(ds.shape, dtype=dt)
                hsmls[ptype][:] = ds
            else:
                hsmls[ptype] = 0.0
            
        return coords, hsmls  
        
    def read_chunk_fields(self,hsmls):    
        # for each field 
        chunk_field_data = {}        
        for ptype, field_list in sorted(self.ptf.items()):
            if self.total_particles[ptype] == 0:
                continue
            for field in field_list:   
                # print(f"processing {ptype}, {field}") 
                if field == "smoothing_length":
                    data = hsmls[ptype]
                else:
                    data = self.read_chunk_field(ptype,field)
                if self.subchunk_size is None:
                    chunk_field_data[(ptype,field)] = data
                else:
                    if data.ndim > 1:
                        subchunk_shape = (self.subchunk_size,1) 
                    else:
                        subchunk_shape = (self.subchunk_size)  
                    chunk_field_data[(ptype,field)] = da.from_array(data,chunks=subchunk_shape)    
        return chunk_field_data
            
    def read_chunk_field(self,ptype,field):
        
        si = self.start 
        ei = self.end
        g = self.f[f"{ptype}"]
        if field in ("Mass", "Masses") and ptype not in self.var_mass:
            data =g["Massarr"][si:ei]
        elif field in self._element_names:
            rfield = "ElementAbundance/" + field
            data = g[rfield][si:ei] 
        elif field.startswith("Metallicity_"):
            col = int(field.rsplit("_", 1)[-1])
            data = g["Metallicity"][si:ei, col] 
        elif field.startswith("GFM_Metals_"):
            col = int(field.rsplit("_", 1)[-1])
            data = g["GFM_Metals"][si:ei, col] 
        elif field.startswith("Chemistry_"):
            col = int(field.rsplit("_", 1)[-1])
            data = g["ChemistryAbundances"][si:ei, col]         
        else:
            data = g[field][si:ei]
                    
        return data 

# selector needs to be serializable to delay this!!!!!! so this fails cause of the cython
@dask.delayed 
def get_chunk_masks(ptf,chunk,selector):

    if selector is None: 
        selector = MockSelector() 

    chunk_masks = {}
    for ptype, field_list in sorted(ptf.items()):

        if getattr(selector, "is_all_data", False):
            mask = slice(None, None, None)
            mask_sum = chunk[2][ptype] # number of elements in this chunk 
            hsmls = None
        else:            
            coords = np.array(chunk[0][ptype]) # coords needs to be a plain np array.....                
            hsmls = chunk[3][ptype]
            mask = selector.select_points(
                coords[:, 0], coords[:, 1], coords[:, 2], hsmls
            )
            if mask is None:
                mask_sum = None 
            else:
                mask_sum = mask.sum() # the size of the masked chunk 

        chunk_masks[ptype] = (mask,mask_sum)

    return chunk_masks
