import yt
from dask.distributed import Client 
from dask_chunking import gadget as ga
import sys 
import time 
        
if __name__=='__main__':
    # spin up the dask client  (NEEDS to be within __main__)
    
    c = None 
    if len(sys.argv)>1 and int(sys.argv[1]) > 1:     
        print(f"Starting dask Client with {sys.argv[1]} processors")    
        c = Client(threads_per_worker=1,n_workers=int(sys.argv[1]))
        print("client started...")


    # load in gadget, set a selection 
    ds = yt.load_sample("snapshot_033")
    ptf = {'PartType0': ['Mass']}    
    sp = ds.sphere(ds.domain_center,(2,'code_length')) 

    select_time = time.time()
    glob_stats, chunk_stats = ga.selector_stats(ds,ptf,sp.selector)
    # glob_stats, chunk_stats = ga.selector_stats(ds,ptf) # for no selector
    select_time = time.time() - select_time
    
    print("\nlocal stats for each chunk:")
    for ch in chunk_stats:
        print(ch)

    print("\nglobal, cross-chunk stats")
    print(glob_stats)

    print(f"\nCompute time (neglecting Client spinup and yt initialization): {select_time}s")
    
    if c is not None:
        print("\nshutting down dask client")
        c.shutdown()
