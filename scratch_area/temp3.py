from  scipy.spatial import Voronoi, voronoi_plot_2d
from descartes import PolygonPatch
from matplotlib import pyplot as plt
import numpy as np
import shapely.geometry
import shapely.ops
import shapely
from shapely.geometry import Point

p = [[b'Colombo', 6.898158, 79.8653],
     [b'IBATTARA3', 6.89, 79.86],
     [b'Isurupaya', 6.89, 79.92],
     [b'Borella', 6.93, 79.86],
     [b'Kompannaveediya', 6.92, 79.85],
     ]

points = np.array(p, dtype=object)

vor = Voronoi(np.flip(points[:, 1:3], 1))
voronoi_plot_2d(vor)

lines = [
    shapely.geometry.LineString(vor.vertices[line])
    for line in vor.ridge_vertices
]
point = Point(6.91, 79.91)
for poly in shapely.ops.polygonize(lines):
    print(point.within(poly))

polyy = shapely.ops.polygonize(lines)




# fig = plt.figure(1)
# PolygonPatch(polyy)