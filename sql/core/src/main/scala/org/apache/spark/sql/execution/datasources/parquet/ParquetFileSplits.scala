/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import java.lang.Iterable
import java.util
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ParquetMetadata}
import org.roaringbitmap.RoaringBitmap
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils

trait ParquetFileSplits {
  def getSplits(stat: FileStatus, filters: Seq[Filter]): Seq[FileSplit]
}

object ParquetDefaultFileSplits extends ParquetFileSplits {
  override def getSplits(stat: FileStatus, filters: Seq[Filter]): Seq[FileSplit] = {
    Seq(new FileSplit(stat.getPath, 0, stat.getLen, Array.empty))
  }
}

class ParquetMetadataFileSplits(
    val root: Path,
    val metadata: ParquetMetadata,
    val schema: StructType)
  extends ParquetFileSplits
  with Logging {
  private val blocks = metadata.getBlocks.asScala
  private val filterSets: LoadingCache[Filter, RoaringBitmap] =
    CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new CacheLoader[Filter, RoaringBitmap] {
        override def load(k: Filter): RoaringBitmap = loadAll(Seq(k).asJava).get(k)
        override def loadAll(keys: Iterable[_ <: Filter]): util.Map[Filter, RoaringBitmap] = {
          buildFilterBitMaps(keys.asScala.toSeq).asJava
        }
      })

  def contains(path: Path): Boolean = {
    blocks.exists { bmd =>
      new Path(root, bmd.getPath) == path
    }
  }

  override def getSplits(stat: FileStatus, filters: Seq[Filter]): Seq[FileSplit] = {
    if (!contains(stat.getPath)) {
      log.warn(s"Found _metadata file for $root," +
        s" but no entries for blocks in ${stat.getPath}. Retaining whole file.")
      ParquetDefaultFileSplits.getSplits(stat, filters)
    } else {
      val (applied, unapplied) = filters.partition(filterSets.getIfPresent(_) != null)
      val result = if (applied.nonEmpty) {
        val start = System.currentTimeMillis()
        val filtered = filterBlocks(applied, unapplied)
        Future {
          filterSets.getAll(unapplied.asJava)
        }(ParquetMetadataFileSplits.executionContext)
        val end = System.currentTimeMillis()
        filtered
      } else {
        val computed = filterSets.getAll(unapplied.asJava).keySet().asScala.toSeq
        filterBlocks(computed, Seq.empty)
      }

      result.filter { bmd =>
        new Path(root, bmd.getPath) == stat.getPath
      }.map { bmd =>
        val blockPath = new Path(root, bmd.getPath)
        new FileSplit(blockPath, bmd.getStartingPos, bmd.getTotalByteSize, Array.empty)
      }
    }
  }

  private def filterBlocks(existing: Seq[Filter], unapplied: Seq[Filter]): Seq[BlockMetaData] = {
    val bitmap = existing.map(filterSets.get).reduce(RoaringBitmap.and)
    val prefiltered = blocks.zipWithIndex.filter { case(bmd, index) => bitmap.contains(index) }
      .map { _._1 }

    if (unapplied.nonEmpty) {
      val filters: Seq[FilterPredicate] = unapplied.flatMap {
        ParquetFilters.createFilter(schema, _)
      }
      val conjunctive = filters.reduce(FilterApi.and)
      prefiltered.filter { bmd =>
        !StatisticsFilter.canDrop(conjunctive, bmd.getColumns)
      }
    } else {
      prefiltered
    }
  }

  private def buildFilterBitMaps(unapplied: Seq[Filter]): Map[Filter, RoaringBitmap] = {
    val sets = unapplied.flatMap { filter =>
      val bitmap = new RoaringBitmap
      ParquetFilters.createFilter(schema, filter)
        .map((filter, _, bitmap))
    }
    var i = 0
    val blockLen = blocks.size
    while (i < blockLen) {
      val bmd = blocks(i)
      sets.foreach { case (filter, parquetFilter, bitmap) =>
        if (!StatisticsFilter.canDrop(parquetFilter, bmd.getColumns)) {
          bitmap.add(i)
        }
      }
      i += 1
    }
    sets.map { case (filter, _, bitmap) =>
      bitmap.runOptimize()
      filter -> bitmap
    }.toMap
  }
}
object ParquetMetadataFileSplits {
  private val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("parquet-metadata-filter", 4))
}
