# CSM GeoTrellis Tiling issue


### Copy dem_1m_a1_norfolk_vabeach_portsmouth_tile4_WGS.tif to data directory

### Run GenHlz test applications

```
> sbt -mem 23000 run
```

Select the `tutorial.GenHLZ` to run.

This will produce the tile set.

The code is in src/main/scala/tutorial/BuildRdd.scala and src/main/scala/tutorial/GenHlz.scala

### Run Tile Viewer.

The code in `src/main/scala/tutorial/ServeNDVI.scala` will do this.

```
> sbt run
```

Select the `tutorial.ServeNDVI` to run.

Then open `static/index.html` and see the blocked tile result web map.
