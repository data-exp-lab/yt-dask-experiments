# read in chunks into pre-allocated dask arrays THEN filter 
import h5py
from dask import dataframe as df, array as da, delayed, compute
import numpy as np 
import dask
import yt

class MockDs(object):
    def __init__(self,ds):
        self.domain_left_edge = ds.domain_left_edge
        self.domain_right_edge = ds.domain_right_edge
        self.periodicity = ds.periodicity
        
class MockSphere(object):
    # a stripped down sphere that records only the attributes required to initialize the 
    # sphere Selector Object
    def __init__(self,sp):
        self.center = sp.center 
        self.radius = sp.radius
        self.ds = MockDs(sp.ds)
                
class MockSelector:
    is_all_data = True

class MockChunkObject:
    def __init__(self, data_file):
        self.data_files = [data_file]

class MockChunk:
    def __init__(self, data_file):
        self.objs = [MockChunkObject(data_file)]
        
class delayed_gadget(object):    
    def __init__(self,ds, ptf = None, mock_selector = None, subchunk_size = 100000):
        
        # references that we need to read in gadget data 
        self.var_mass = ds.index.io.var_mass
        self._element_names = ds.index.io._element_names
        
        # initialize the chunks 
        self.chunks = [MockChunk(data_file) for data_file in ds.index.data_files]
        self.data_files = self.return_data_files()
        self.subchunk_size = subchunk_size
        self.mock_selector = mock_selector
        self.ptf = ptf 
        
        # containers for delayed objects 
        self.delayed_chunks = [] # full chunks 
        self.masks = [] # only masks 
        self.masked_chunks = [] # masked chunks 
        
        # initialize the delayed on full dataset 
        if ptf is not None:
            self.stage_chunks(ptf)
        
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
            
        self.set_chunk_masks() 
        self.apply_masks()
            
    def set_chunk_masks(self,mock_selector = None):
        # sets the delayed masks by chunk 
        if mock_selector is not None:
            self.mock_selector = mock_selector 
            
        self.masks=[]
        for chunk in self.delayed_chunks:
            self.masks.append(get_chunk_masks(self.ptf,chunk,self.mock_selector))
            
    def apply_masks(self):
        self.masked_chunks = [] 
        for chunk,mask in zip(self.delayed_chunks,self.masks):
            self.masked_chunks.append(delayed_masked_chunk(chunk,mask))
    
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

@dask.delayed 
def get_chunk_masks(ptf,chunk,mock_selection_obj):
    # mock_selection_obj here is a stripped down selection object containing only 
    # the pickable attributes required for initialization of the selection_routines.
    if mock_selection_obj is None: 
        selector = MockSelector()
    else:
        if isinstance(mock_selection_obj,MockSphere):
            selector = yt.geometry.selection_routines.SphereSelector(mock_selection_obj)
        else:
            selector = mock_selection_obj# no longer a mock! 

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
    
@dask.delayed 
def delayed_masked_chunk(chunk,mask):    
    # chunk [coords, data , total_particles, hsmls]    
    data={}
    coords = {}
    for ptype_fld, chunk_data in chunk[1].items(): 
        ptype=ptype_fld[0]
        this_mask = mask[ptype][0]
        data[ptype_fld] = chunk_data[this_mask]
    
    for ptype, coordvals in chunk[0].items():
        this_mask = mask[ptype][0]
        coords[ptype] = coordvals[this_mask] 
        
    return coords, data 
