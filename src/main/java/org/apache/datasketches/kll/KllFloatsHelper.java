/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.kll;

import static org.apache.datasketches.Util.isEven;
import static org.apache.datasketches.Util.isOdd;

import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.SketchesArgumentException;

/**
 * Static methods to support KllFloatsSketch
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
class KllFloatsHelper {

  /**
   * Compression algorithm used to merge higher levels.
   * <p>Here is what we do for each level:</p>
   * <ul><li>If it does not need to be compacted, then simply copy it over.</li>
   * <li>Otherwise, it does need to be compacted, so...
   *   <ul><li>Copy zero or one guy over.</li>
   *       <li>If the level above is empty, halve up.</li>
   *       <li>Else the level above is nonempty, so halve down, then merge up.</li>
   *   </ul></li>
   * <li>Adjust the boundaries of the level above.</li>
   * </ul>
   *
   * <p>It can be proved that generalCompress returns a sketch that satisfies the space constraints
   * no matter how much data is passed in.
   * We are pretty sure that it works correctly when inBuf and outBuf are the same.
   * All levels except for level zero must be sorted before calling this, and will still be
   * sorted afterwards.
   * Level zero is not required to be sorted before, and may not be sorted afterwards.</p>
   *
   * <p>This trashes inBuf and inLevels and modifies outBuf and outLevels.</p>
   *
   * @param k The sketch parameter k
   * @param m The minimum level size
   * @param numLevelsIn provisional number of number of levels = max(this.numLevels, other.numLevels)
   * @param inBuf work buffer of size = this.getNumRetained() + other.getNumRetainedAboveLevelZero().
   * This contains the float[] of the other sketch
   * @param inLevels work levels array size = ubOnNumLevels(this.n + other.n) + 2
   * @param outBuf the same array as inBuf
   * @param outLevels the same size as inLevels
   * @param isLevelZeroSorted true if this.level 0 is sorted
   * @param random instance of java.util.Random
   * @return int array of: {numLevels, targetItemCount, currentItemCount)
   */
  static int[] generalFloatsCompress(
      final int k,
      final int m,
      final int numLevelsIn,
      final float[] inBuf,
      final int[] inLevels,
      final float[] outBuf,
      final int[] outLevels,
      final boolean isLevelZeroSorted,
      final Random random) {
    assert numLevelsIn > 0; // things are too weird if zero levels are allowed
    int numLevels = numLevelsIn;
    int currentItemCount = inLevels[numLevels] - inLevels[0]; // decreases with each compaction
    int targetItemCount = KllHelper.computeTotalItemCapacity(k, m, numLevels); // increases if we add levels
    boolean doneYet = false;
    outLevels[0] = 0;
    int curLevel = -1;
    while (!doneYet) {
      curLevel++; // start out at level 0

      // If we are at the current top level, add an empty level above it for convenience,
      // but do not increment numLevels until later
      if (curLevel == (numLevels - 1)) {
        inLevels[curLevel + 2] = inLevels[curLevel + 1];
      }

      final int rawBeg = inLevels[curLevel];
      final int rawLim = inLevels[curLevel + 1];
      final int rawPop = rawLim - rawBeg;

      if ((currentItemCount < targetItemCount) || (rawPop < KllHelper.levelCapacity(k, numLevels, curLevel, m))) {
        // copy level over as is
        // because inBuf and outBuf could be the same, make sure we are not moving data upwards!
        assert (rawBeg >= outLevels[curLevel]);
        System.arraycopy(inBuf, rawBeg, outBuf, outLevels[curLevel], rawPop);
        outLevels[curLevel + 1] = outLevels[curLevel] + rawPop;
      }
      else {
        // The sketch is too full AND this level is too full, so we compact it
        // Note: this can add a level and thus change the sketch's capacity

        final int popAbove = inLevels[curLevel + 2] - rawLim;
        final boolean oddPop = isOdd(rawPop);
        final int adjBeg = oddPop ? 1 + rawBeg : rawBeg;
        final int adjPop = oddPop ? rawPop - 1 : rawPop;
        final int halfAdjPop = adjPop / 2;

        if (oddPop) { // copy one guy over
          outBuf[outLevels[curLevel]] = inBuf[rawBeg];
          outLevels[curLevel + 1] = outLevels[curLevel] + 1;
        } else { // copy zero guys over
          outLevels[curLevel + 1] = outLevels[curLevel];
        }

        // level zero might not be sorted, so we must sort it if we wish to compact it
        if ((curLevel == 0) && !isLevelZeroSorted) {
          Arrays.sort(inBuf, adjBeg, adjBeg + adjPop);
        }

        if (popAbove == 0) { // Level above is empty, so halve up
          randomlyHalveUpFloats(inBuf, adjBeg, adjPop, random);
        } else { // Level above is nonempty, so halve down, then merge up
          randomlyHalveDownFloats(inBuf, adjBeg, adjPop, random);
          mergeSortedFloatArrays(inBuf, adjBeg, halfAdjPop, inBuf, rawLim, popAbove, inBuf, adjBeg + halfAdjPop);
        }

        // track the fact that we just eliminated some data
        currentItemCount -= halfAdjPop;

        // Adjust the boundaries of the level above
        inLevels[curLevel + 1] = inLevels[curLevel + 1] - halfAdjPop;

        // Increment numLevels if we just compacted the old top level
        // This creates some more capacity (the size of the new bottom level)
        if (curLevel == (numLevels - 1)) {
          numLevels++;
          targetItemCount += KllHelper.levelCapacity(k, numLevels, 0, m);
        }
      } // end of code for compacting a level

      // determine whether we have processed all levels yet (including any new levels that we created)
      if (curLevel == (numLevels - 1)) { doneYet = true; }
    } // end of loop over levels

    assert (outLevels[numLevels] - outLevels[0]) == currentItemCount;
    return new int[] {numLevels, targetItemCount, currentItemCount};
  }

  static void mergeSortedFloatArrays(
      final float[] bufA, final int startA, final int lenA,
      final float[] bufB, final int startB, final int lenB,
      final float[] bufC, final int startC) {
    final int lenC = lenA + lenB;
    final int limA = startA + lenA;
    final int limB = startB + lenB;
    final int limC = startC + lenC;

    int a = startA;
    int b = startB;

    for (int c = startC; c < limC; c++) {
      if (a == limA) {
        bufC[c] = bufB[b];
        b++;
      } else if (b == limB) {
        bufC[c] = bufA[a];
        a++;
      } else if (bufA[a] < bufB[b]) {
        bufC[c] = bufA[a];
        a++;
      } else {
        bufC[c] = bufB[b];
        b++;
      }
    }
    assert a == limA;
    assert b == limB;
  }

  //This must be modified for validation
  static void randomlyHalveDownFloats(final float[] buf, final int start, final int length, final Random random) {
    assert isEven(length);
    final int half_length = length / 2;
    final int offset = random.nextInt(2);       // disable for validation
    //final int offset = deterministicOffset(); // enable for validation
    int j = start + offset;
    for (int i = start; i < (start + half_length); i++) {
      buf[i] = buf[j];
      j += 2;
    }
  }

  //This must be modified for validation
  static void randomlyHalveUpFloats(final float[] buf, final int start, final int length, final Random random) {
    assert isEven(length);
    final int half_length = length / 2;
    final int offset = random.nextInt(2);       // disable for validation
    //final int offset = deterministicOffset(); // enable for validation
    int j = (start + length) - 1 - offset;
    for (int i = (start + length) - 1; i >= (start + half_length); i--) {
      buf[i] = buf[j];
      j -= 2;
    }
  }

  /**
   * Checks the sequential validity of the given array of float values.
   * They must be unique, monotonically increasing and not NaN.
   * @param values the given array of values
   */
  static void validateFloatValues(final float[] values) {
    for (int i = 0; i < values.length; i++) {
      if (!Float.isFinite(values[i])) {
        throw new SketchesArgumentException("Values must be finite");
      }
      if (i < values.length - 1 && values[i] >= values[i + 1]) {
        throw new SketchesArgumentException(
          "Values must be unique and monotonically increasing");
      }
    }
  }

  /*
   * The following must be enabled for use with the KllFloatsValidationTest,
   * which is only enabled for manual testing. In addition, two methods
   * above need to be modified as commented.
   */

  //  static int nextOffset = 0;
  //
  //  private static int deterministicOffset() {
  //    final int result = nextOffset;
  //    nextOffset = 1 - nextOffset;
  //    return result;
  //  }

}
