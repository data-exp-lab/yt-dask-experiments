from dask_chunking import gadget_da as gda
import yt
from dask.distributed import Client 
from dask import compute 
import sys 
import time 

if __name__=='__main__':
    # spin up the dask client  (NEEDS to be within __main__)    
    c = None 
    if len(sys.argv)>1 and int(sys.argv[1]) > 1:     
        print(f"Starting dask Client with {sys.argv[1]} processors")    
        c = Client(threads_per_worker=1,n_workers=int(sys.argv[1]))
        print("client started...")

    ds = yt.load_sample("snapshot_033")
    sp = ds.sphere(ds.domain_center,(2,'code_length')) 
    ptf = {'PartType0': ['Mass']} 
    mock_sphere = gda.MockSphere(sp)
    delayed_reader = gda.delayed_gadget(ds, ptf, mock_selector = mock_sphere, subchunk_size = None)
    # delayed_reader.delayed_chunks[0].compute()
    # data = compute(*delayed_reader.delayed_chunks)
    data_subset = compute(*delayed_reader.masked_chunks)
    # delayed_reader.set_chunk_masks("snapshot_033")
    # masks = compute(*delayed_reader.masks)
    
    # print(f"\nCompute time (neglecting Client spinup and yt initialization): {select_time}s")
    # 
    if c is not None:
        print("\nshutting down dask client")
        c.shutdown()
