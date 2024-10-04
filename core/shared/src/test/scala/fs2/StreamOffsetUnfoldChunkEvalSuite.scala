/*
 * Copyright (c) 2013 Functional Streams for Scala
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package fs2

import cats.*
import cats.effect.IO
import cats.syntax.all.*
import munit.*
import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen, Prop, Shrink}

import scala.annotation.*
import scala.collection.immutable

class StreamOffsetUnfoldChunkEvalSuite extends CatsEffectSuite with ScalaCheckEffectSuite {
  private def unfoldFunction[F[_]: Applicative](
      chunkCount: ChunkCount,
      chunkSize: ChunkSize,
      effect: F[Unit]
  )(maybeToken: Option[Long]): F[(Chunk[Long], Option[Long])] = {
    val offset = maybeToken.getOrElse(0L)
    val ints =
      Stream
        .iterate(0L)(_ + 1)
        .chunkN(chunkSize.value)
        .drop(offset)
        .take(Math.min(1, chunkCount.value).toLong)
        .unchunks
        .to(Chunk)

    val nextToken = offset + 1

    effect.as(ints -> Option(nextToken).filter(_ < chunkCount.value))
  }

  test("offsetUnfoldChunkEval should lazily unfold up to an arbitrary take limit") {
    PropF.forAllF { (takeLimit: TakeLimit, chunkCount: ChunkCount, chunkSize: ChunkSize) =>
      Counter[IO]
        .flatMap { effectCounter =>
          Stream
            .offsetUnfoldChunkEval(
              unfoldFunction(chunkCount, chunkSize, effectCounter.increment)
            )
            .take(takeLimit.value.toLong)
            .compile
            .toList
            .product(effectCounter.get)
            .map { case (output, effectCount) =>
              val expectedEffectCount =
                if (takeLimit.isGreaterThanAvailableData(chunkCount, chunkSize))
                  Math.max(1, chunkCount.value)
                else takeLimit.effectsExecutedGiven(chunkSize)

              assertEquals(effectCount, expectedEffectCount)
              assertEquals(output, 0 until expectedLimit(takeLimit, chunkCount, chunkSize))
            }
        }
    }
  }

  test("offsetUnfoldChunkEval should unfold up to an arbitrary take limit with a pure Stream") {
    Prop.forAll { (takeLimit: TakeLimit, chunkCount: ChunkCount, chunkSize: ChunkSize) =>
      val output = Stream
        .offsetUnfoldChunkEval(unfoldFunction[Id](chunkCount, chunkSize, ()))
        .take(takeLimit.value.toLong)
        .compile
        .toList

      assertEquals(output, 0 until expectedLimit(takeLimit, chunkCount, chunkSize))
    }
  }

  private implicit def compareListAndRange[A: Integral]: Compare[List[A], Range] =
    _.map(implicitly[Integral[A]].toInt) == _.toList

  private implicit def compareLongAndInt: Compare[Long, Int] =
    Math.toIntExact(_) == _

  private def expectedLimit(
      takeLimit: TakeLimit,
      chunkCount: ChunkCount,
      chunkSize: ChunkSize
  ): Int =
    if (chunkCount * chunkSize == 0) 0
    else if (takeLimit.isGreaterThanAvailableData(chunkCount, chunkSize))
      chunkCount * chunkSize
    else takeLimit.value
}

case class TakeLimit(value: Int) {
  def isGreaterThanAvailableData(chunkCount: ChunkCount, chunkSize: ChunkSize): Boolean =
    this.value > (chunkCount * chunkSize)

  def canBeEvenlyDividedIntoChunksOfSize(chunkSize: ChunkSize): Boolean =
    value % chunkSize.value == 0

  def effectsExecutedGiven(chunkSize: ChunkSize): Int =
    (value / chunkSize.value) + (if (canBeEvenlyDividedIntoChunksOfSize(chunkSize)) 0 else 1)
}
object TakeLimit {
  implicit val arbTakeLimit: Arbitrary[TakeLimit] = Arbitrary(Gen.posNum[Int].map(TakeLimit(_)))
  @nowarn("cat=deprecation")
  implicit val shrinkTakeLimit: Shrink[TakeLimit] = Shrink { (x: TakeLimit) =>
    immutable.Stream
      .iterate(x.value / 2)(_ / 2)
      .takeWhile(_ > 0)
      .map(TakeLimit(_))
  }
}

case class ChunkCount(value: Int) {
  def *(chunkSize: ChunkSize): Int =
    Math.multiplyExact(value, chunkSize.value)
}
object ChunkCount {
  implicit val arbTakeLimit: Arbitrary[ChunkCount] = Arbitrary(
    Gen.chooseNum(0, 1000).map(ChunkCount(_))
  )
  @nowarn("cat=deprecation")
  implicit val shrinkTakeLimit: Shrink[ChunkCount] = Shrink { (x: ChunkCount) =>
    immutable.Stream
      .iterate(x.value / 2)(_ / 2)
      .takeWhile(_ > 0)
      .map(ChunkCount(_))
  }
}

case class ChunkSize(value: Int)
object ChunkSize {
  implicit val arbTakeLimit: Arbitrary[ChunkSize] = Arbitrary(
    Gen.chooseNum(1, 10).map(ChunkSize(_))
  )
  @nowarn("cat=deprecation")
  implicit val shrinkTakeLimit: Shrink[ChunkSize] = Shrink { (x: ChunkSize) =>
    immutable.Stream
      .iterate(x.value / 2)(_ / 2)
      .takeWhile(_ > 0)
      .map(ChunkSize(_))
  }
}
