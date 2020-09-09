# Note that this requires yt 4!
import yt
import contextlib
from collections import defaultdict

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

# So what we see is that the result of _identify_base_chunk has been popped
# into the region as the _current_chunk.
#
# A lot of this happens in the `_chunked_read` function, which is a context
# manager that tracks objects and fields.
#
# It's a little tricky to instrument this because it's a context manager.  But,
# if we instrument it with just a print statement, we can see when and how it's
# called.
#
#   @contextmanager
#   def _chunked_read(self, chunk):
#       # There are several items that need to be swapped out
#       # field_data, size, shape
#       obj_field_data = []
#       if hasattr(chunk, "objs"):
#           for obj in chunk.objs:
#               obj_field_data.append(obj.field_data)
#               obj.field_data = YTFieldData()
#       old_field_data, self.field_data = self.field_data, YTFieldData()
#       old_chunk, self._current_chunk = self._current_chunk, chunk
#       old_locked, self._locked = self._locked, False
#       yield
#       self.field_data = old_field_data
#       self._current_chunk = old_chunk
#       self._locked = old_locked
#       if hasattr(chunk, "objs"):
#           for obj in chunk.objs:
#               obj.field_data = obj_field_data.pop(0)

levels = {'level': 0} # make it mutable

def trace_chunked_read(func):
    space ="  "
    @contextlib.contextmanager
    def _func(self, *args, **kwargs):
        levels['level'] += 1
        print(f"{space * levels['level']}Entering chunked_read at level  {levels['level']}")
        yield func(self, *args, **kwargs)
        print(f"{space * levels['level']}Exiting chunked_read from level {levels['level']}")
        levels['level'] -= 1
    return _func

reg = ds.r[0.1:0.2,0.1:0.2,0.1:0.2]

reg._chunked_read = trace_chunked_read(reg._chunked_read)
reg["density"]
for chunk_index, chunk in enumerate(reg.chunks([], "io")):
    field = reg["density"]
    print(f"{chunk_index=} with {field.size=}")

# We do this to demonstrate something that happens much more frequently in the context of 

print("\n\nNow, we run it again, with a layered chunking.\n")

levels = defaultdict(lambda: 0)

for chunk in reg.chunks([], "all"):
    for subchunk in reg.chunks([], "io"):
        print(f"{chunk_index=} with {field.size=}")

# This doesn't prove that *much* except that we have a layered chunking system.
# What we want to take a look at is how the chunking process works.


