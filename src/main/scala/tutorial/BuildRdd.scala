package tutorial

/**
 * Created by chrismangold on 2/23/16.
 */

import java.io.File
import javax.xml.ws.WebEndpoint

import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.proj4.{WebMercator, LatLng}
import geotrellis.raster._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.render._
import geotrellis.raster.reproject.Reproject.Options
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.SpatialKey
import geotrellis.spark.io.hadoop.formats.GeotiffInputFormat
import geotrellis.vector.{ProjectedExtent, Point, Extent}
import org.apache.hadoop.fs.Path
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import geotrellis.raster.reproject.Reproject.{Options => RasterReprojectOptions}

import geotrellis.engine._
import scala.collection.mutable.ListBuffer

import java.nio.file.{Files, Paths}

/**
 * Created by chrismangold on 1/8/16.
 */
object BuildRdd {

  val defaultTiffExtensions: Seq[String] = Seq(".tif", ".TIF", ".tiff", ".TIFF")


  def parseUri( uri:String) : ( String, String)  = {

    if (uri.toLowerCase().contains("s3")) {

      // Then get Bucketname
      val rootStr= uri.substring(("s3://".length) )
      println(rootStr)
      val sepIndex = rootStr.indexOf('/')
      val bucketName = rootStr.substring(0, sepIndex  )
      val filePath = rootStr.substring(sepIndex + 1 )

      (bucketName, filePath)

    } else if (uri.toLowerCase().contains("hdfs")) {
      val rootStr= uri.substring(("hdfs://".length) )
      println(rootStr)
      //val sepIndex = rootStr.indexOf('/')
      // val bucketName = rootStr.substring(0, sepIndex  )
      //val filePath = rootStr.substring(sepIndex + 1 )

      ("hdfs",rootStr)
    } else {
      ("empty","empty")
    }

  }

  def tilePrepRoute( implicit sc: SparkContext ): RDD[ (ProjectedExtent, Tile) ] =  {


    var tileList= new ListBuffer[(ProjectedExtent,Tile)]()
    var bytesToUse: Array[Byte] = null

    var fileCount = 0
    var totalTileSize = 0

    var firstFile = "data/dem_1m_a1_norfolk_vabeach_portsmouth_tile4_WGS.tif"
    println( "File name is  " + firstFile )

    if (firstFile.length > 0) {
      bytesToUse = Files.readAllBytes(Paths.get(firstFile.toString))
    } else {
      throw new Exception("Land cover layer not supplied");
    }

    // Construct an object with instructions to fetch the raster
    // Returns SingleBandGeoTiff
    val gtIn = SinglebandGeoTiff(bytesToUse)

    val projectOptions = RasterReprojectOptions(Bilinear)
    val projectedRaster = gtIn.projectedRaster.reproject( WebMercator, projectOptions)

    val projExt = new ProjectedExtent( projectedRaster._2, WebMercator)

    var newEntry = (projExt, projectedRaster._1)

    tileList += newEntry

    // Look at slope on the single tile.
    val slopeTile:Tile = projectedRaster._1.slope(projectedRaster.rasterExtent.cellSize, 1.0)
    var testRamp1 = ColorRamps.LightToDarkSunset.toColorMap(slopeTile.histogram)
    slopeTile.renderPng(testRamp1).write("data/tileslope.png")

    println( "Parallelizing files.")

    val tiles = sc.parallelize( tileList, 4 )

    tiles

  }


}
