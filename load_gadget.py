# Note that this requires yt 4!
import yt

ds = yt.load_sample("snapshot_033")
print(ds.index)

reg = ds.r[0.25:0.5, 0.5:0.75, 0.2:0.25]
for chunk in reg.chunks([], "io"):
    print(chunk)

# So what has just happened is that `_identify_base_chunk` was called, and that
# creates a list of base chunk objects.  We can see this with a function
# decorator:

def print_input_output(func):
    def _func(*args, **kwargs):
        print(f"Entering {func.__name__}")
        for i, a in enumerate(args):
            print(f" arg {i}: {a}")
        for k, v in sorted(kwargs.items()):
            print(f" {k} = {v}")
        rv = func(*args, **kwargs)
        print(f"Leaving {func.__name__}")
        print(f"  return value: {rv}")
        return rv
    return _func

ds.index._identify_base_chunk = print_input_output(ds.index._identify_base_chunk)
reg = ds.r[0.25:0.5, 0.5:0.75, 0.2:0.25]
reg["density"]

# When we do this, note that what is supplied is the YTRegion object, and what
# is returned is ... None!  That's because the index object modifies the
# YTRegion object in place.

def print_chunk_info(dobj, indent = 0):
    space = indent * " "
    print(f"{space}{dobj._current_chunk=}")
    print(f"{space}{dobj._current_chunk.chunk_type=}")
    print(f"{space}{dobj._current_chunk.data_size=}")
    print(f"{space}{dobj._current_chunk.dobj=}")
    print(f"{space}{len(dobj._current_chunk.objs)=} of type {set(type(_) for _ in dobj._current_chunk.objs)}")

# Let's look at what this looks like for chunking a data object.  Note that
# what happens here is that we see the current_chunk for each subchunk.

reg = ds.r[0.1:0.2,0.1:0.2,0.1:0.2]
for i, chunk in enumerate(reg.chunks([], "io")):
    print()
    print(f" chunk {i}")
    print_chunk_info(reg, 4)

# Let's try now what happens if we look at current chunk *after* we access a data field.
print_chunk_info(reg, 0)
