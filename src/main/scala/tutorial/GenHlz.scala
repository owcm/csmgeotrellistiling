package tutorial

import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import geotrellis.raster.reproject.Reproject.Options
import geotrellis.raster.resample._

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.avro.codecs._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling.{ZoomedLayoutScheme, FloatingLayoutScheme}

import geotrellis.vector._

import akka.actor._
import akka.io.IO
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd.RDD
import spray.can.Http
import spray.routing.{HttpService, RequestContext}
import spray.routing.directives.CachingDirectives
import spray.http.MediaTypes
import scala.concurrent._
import com.typesafe.config.ConfigFactory
import geotrellis.raster.reproject.Reproject.{Options => RasterReprojectOptions}

/**
 * Created by chrismangold on 2/23/16.
 */

object GenHlz {

  val outputPath = fullPath("data/tiles")

  def main(args: Array[String]): Unit = {
    // Setup Spark to use Kryo serializer.
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")
        .set("spark.kryoserializer.buffer.max", "512m")

    val sc = new SparkContext(conf)

    try {
      run(sc)
      // Pause to wait to close the spark context,
      // so that you can check out the UI at http://localhost:4040
      println("Hit enter to exit.")
      readLine()
    } finally {
      sc.stop()
    }
  }

  def fullPath(path: String) = new java.io.File(path).getAbsolutePath

  def run(implicit sc: SparkContext) = {

    val inputTiles = BuildRdd.tilePrepRoute( sc)

    println("Processing Set FloatingLayoutScheme")
    val (_, tileLayerMetadata ) = TileLayerMetadata.fromRdd(inputTiles, FloatingLayoutScheme(512))

    val tiled: RDD[(SpatialKey, Tile)] =
      inputTiles
        .tileToLayout(tileLayerMetadata.cellType, tileLayerMetadata.layout, Bilinear)
        .repartition(100)

    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    println("Derived Zoom and Raster Data")

    val projectOptions = RasterReprojectOptions(Bilinear)
    val (zoom, reprojected): (Int, RDD[(SpatialKey, Tile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      TileLayerRDD(tiled, tileLayerMetadata)
        .reproject(WebMercator, layoutScheme, Bilinear)

    println("Zoom is " + zoom )
    println("Cell size is " + tileLayerMetadata.layout.cellSize.resolution)

    // Review Stich of original tiles
    val stitchip = tiled.stitch
    var testRamp1 = ColorRamps.LightToDarkSunset.toColorMap(stitchip.histogram)
    stitchip.renderPng(testRamp1).write("data/stitchedip.png")

    // Review Stich of original Reprojected Tiles
    val stitchre = reprojected.stitch
    stitchre._1.renderPng(testRamp1).write("data/stitchedre.png")

    val slopeRdd = reprojected.slope(1.0)

    // Review slope RDD as Stitch
    //  val stitch = slopeRdd.stitch
    //  stitch._1.renderPng(testRamp1).write("data/stitched.png")

    // Create the attributes store that will tell us information about our catalog.
    val attributeStore = FileAttributeStore(outputPath)

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer = FileLayerWriter(attributeStore)

    val writeOp =
    Pyramid.upLevels(slopeRdd, layoutScheme, zoom) { (rdd, z) =>

      val layerId = LayerId("landsat", z)
      // If the layer exists already, delete it out before writing
      if(attributeStore.layerExists(layerId)) {
        new FileLayerManager(attributeStore).delete(layerId)
      }

      writer.write(layerId, rdd, ZCurveKeyIndexMethod)

    }

  }

}

