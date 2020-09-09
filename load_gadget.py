# Note that this requires yt 4!
import yt

ds = yt.load_sample("snapshot_033")
print(ds.index)
