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

import static org.apache.datasketches.Family.idToFamily;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_DOUBLE;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_FLOAT;
import static org.apache.datasketches.kll.KllPreambleUtil.DATA_START_ADR_SINGLE_ITEM;
import static org.apache.datasketches.kll.KllPreambleUtil.DEFAULT_M;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_DOUBLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_EMPTY_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.PREAMBLE_INTS_FLOAT;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_EMPTY_FULL;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_SINGLE;
import static org.apache.datasketches.kll.KllPreambleUtil.SERIAL_VERSION_UPDATABLE;
import static org.apache.datasketches.kll.KllPreambleUtil.extractDoubleSketchFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.extractDyMinK;
import static org.apache.datasketches.kll.KllPreambleUtil.extractEmptyFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.extractFamilyID;
import static org.apache.datasketches.kll.KllPreambleUtil.extractFlags;
import static org.apache.datasketches.kll.KllPreambleUtil.extractK;
import static org.apache.datasketches.kll.KllPreambleUtil.extractLevelZeroSortedFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.extractM;
import static org.apache.datasketches.kll.KllPreambleUtil.extractN;
import static org.apache.datasketches.kll.KllPreambleUtil.extractNumLevels;
import static org.apache.datasketches.kll.KllPreambleUtil.extractPreInts;
import static org.apache.datasketches.kll.KllPreambleUtil.extractSerVer;
import static org.apache.datasketches.kll.KllPreambleUtil.extractSingleItemFlag;
import static org.apache.datasketches.kll.KllPreambleUtil.extractUpdatableFlag;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.kll.KllPreambleUtil.Layout;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This class performs all the error checking of an incoming Memory object and extracts the key fields in the process.
 * This is used by all sketches that read or import Memory objects.
 *
 * @author lrhodes
 *
 */
final class MemoryValidate {
  // first 8 bytes
  final int preInts; // = extractPreInts(srcMem);
  final int serVer;
  final int familyID;
  final String famName;
  final int flags;
  boolean empty;
  boolean singleItem;
  final boolean level0Sorted;
  final boolean doublesSketch;
  final boolean updatable;
  final int k;
  final int m;
  final int memCapacity;

  Layout layout;
  // depending on the layout, the next 8-16 bytes of the preamble, may be filled with assumed values.
  // For example, if the layout is compact & empty, n = 0, if compact and single, n = 1, etc.
  long n;
  // next 4 bytes
  int dyMinK;
  int numLevels;
  // derived
  int capacityItems; //capacity of Items array for exporting and for Updatable form
  int itemsRetained; //actual items retained in Compact form
  int itemsArrStart;
  int sketchBytes;
  Memory levelsArrCompact; //if sk = empty or single, this is derived
  Memory minMaxArrCompact; //if sk = empty or single, this is derived
  Memory itemsArrCompact;  //if sk = empty or single, this is derived
  WritableMemory levelsArrUpdatable;
  WritableMemory minMaxArrUpdatable;
  WritableMemory itemsArrUpdatable;

  MemoryValidate(final Memory srcMem) {
    memCapacity = (int) srcMem.getCapacity();
    preInts = extractPreInts(srcMem);
    serVer = extractSerVer(srcMem);

    familyID = extractFamilyID(srcMem);
    if (familyID != Family.KLL.getID()) { memoryValidateThrow(0, familyID); }
    famName = idToFamily(familyID).toString();
    flags = extractFlags(srcMem);
    empty = extractEmptyFlag(srcMem);
    level0Sorted  = extractLevelZeroSortedFlag(srcMem);
    singleItem    = extractSingleItemFlag(srcMem);
    doublesSketch = extractDoubleSketchFlag(srcMem);
    updatable    = extractUpdatableFlag(srcMem);
    k = extractK(srcMem);
    KllHelper.checkK(k);
    m = extractM(srcMem);
    if (m != DEFAULT_M) { memoryValidateThrow(7, m); }
    if ((serVer == SERIAL_VERSION_UPDATABLE) ^ updatable) { memoryValidateThrow(10, 0); }

    if (updatable) { updatableMemoryValidate((WritableMemory) srcMem); }
    else { compactMemoryValidate(srcMem); }
  }

  void compactMemoryValidate(final Memory srcMem) {
    if (empty && singleItem) { memoryValidateThrow(20, 0); }
    final int sw = (empty ? 1 : 0) | (singleItem ? 4 : 0) | (doublesSketch ? 8 : 0);
    switch (sw) {
      case 0: { //Float Compact FULL
        if (preInts != PREAMBLE_INTS_FLOAT) { memoryValidateThrow(6, preInts); }
        if (serVer != SERIAL_VERSION_EMPTY_FULL) { memoryValidateThrow(2, serVer); }

        layout = Layout.FLOAT_FULL_COMPACT;
        n = extractN(srcMem);
        dyMinK = extractDyMinK(srcMem);
        numLevels = extractNumLevels(srcMem);
        int offset = DATA_START_ADR_FLOAT;
        // LEVELS MEM
        final int[] myLevelsArr = new int[numLevels + 1];
        srcMem.getIntArray(offset, myLevelsArr, 0, numLevels); //copies all except the last one
        myLevelsArr[numLevels] = KllHelper.computeTotalItemCapacity(k, m, numLevels); //load the last one
        levelsArrCompact = Memory.wrap(myLevelsArr); //separate from srcMem,
        offset += (int)levelsArrCompact.getCapacity() - Integer.BYTES; // but one larger than srcMem
        // MIN/MAX MEM
        minMaxArrCompact = srcMem.region(offset, 2L * Float.BYTES);
        offset += (int)minMaxArrCompact.getCapacity();
        // ITEMS MEM
        itemsArrStart = offset;
        capacityItems = myLevelsArr[numLevels];
        itemsRetained = capacityItems - myLevelsArr[0];
        final float[] myItemsArr = new float[capacityItems];
        srcMem.getFloatArray(offset, myItemsArr, myLevelsArr[0], itemsRetained);
        itemsArrCompact = Memory.wrap(myItemsArr);
        sketchBytes = offset + itemsRetained * Float.BYTES;
        break;
      }
      case 1: { //Float Compact EMPTY
        if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { memoryValidateThrow(1, preInts); }
        if (serVer != SERIAL_VERSION_EMPTY_FULL) { memoryValidateThrow(2, serVer); }
        layout = Layout.FLOAT_EMPTY_COMPACT;
        n = 0;           //assumed
        dyMinK = k;      //assumed
        numLevels = 1;   //assumed
        // LEVELS MEM
        levelsArrCompact = Memory.wrap(new int[] {k, k});
        // MIN/MAX MEM
        minMaxArrCompact = Memory.wrap(new float[] {Float.NaN, Float.NaN});
        // ITEMS MEM
        capacityItems = k;
        itemsRetained = 0;
        itemsArrCompact = Memory.wrap(new float[k]);
        sketchBytes = DATA_START_ADR_SINGLE_ITEM; //also used for empty
        itemsArrStart = DATA_START_ADR_SINGLE_ITEM;
        break;
      }
      case 4: { //Float Compact SINGLE
        if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { memoryValidateThrow(1, preInts); }
        if (serVer != SERIAL_VERSION_SINGLE) { memoryValidateThrow(4, serVer); }
        layout = Layout.FLOAT_SINGLE_COMPACT;
        n = 1;
        dyMinK = k;
        numLevels = 1;
        // LEVELS MEM
        levelsArrCompact = Memory.wrap(new int[] {k - 1, k});
        final float minMax = srcMem.getFloat(DATA_START_ADR_SINGLE_ITEM);
        // MIN/MAX MEM
        minMaxArrCompact = Memory.wrap(new float[] {minMax, minMax});
        // ITEMS MEM
        capacityItems = k;
        itemsRetained = 1;
        final float[] myFloatItems = new float[k];
        myFloatItems[k - 1] = minMax;
        itemsArrCompact = Memory.wrap(myFloatItems);
        sketchBytes = DATA_START_ADR_SINGLE_ITEM + Float.BYTES;
        itemsArrStart = DATA_START_ADR_SINGLE_ITEM;
        break;
      }
      case 8: { //Double Compact FULL
        if (preInts != PREAMBLE_INTS_DOUBLE) { memoryValidateThrow(5, preInts); }
        if (serVer != SERIAL_VERSION_EMPTY_FULL) { memoryValidateThrow(2, serVer); }
        layout = Layout.DOUBLE_FULL_COMPACT;
        n = extractN(srcMem);
        dyMinK = extractDyMinK(srcMem);
        numLevels = extractNumLevels(srcMem);
        int offset = DATA_START_ADR_DOUBLE;
        // LEVELS MEM
        final int[] myLevelsArr = new int[numLevels + 1];
        srcMem.getIntArray(offset, myLevelsArr, 0, numLevels); //all except the last one
        myLevelsArr[numLevels] = KllHelper.computeTotalItemCapacity(k, m, numLevels); //load the last one
        levelsArrCompact = Memory.wrap(myLevelsArr); //separate from srcMem
        offset += (int)levelsArrCompact.getCapacity() - Integer.BYTES;
        // MIN/MAX MEM
        minMaxArrCompact = srcMem.region(offset, 2L * Double.BYTES);
        offset += (int)minMaxArrCompact.getCapacity();
        // ITEMS MEM
        itemsArrStart = offset;
        capacityItems = myLevelsArr[numLevels];
        itemsRetained = capacityItems - myLevelsArr[0];
        final double[] myItemsArr = new double[capacityItems];
        srcMem.getDoubleArray(offset, myItemsArr, myLevelsArr[0], itemsRetained);
        itemsArrCompact = Memory.wrap(myItemsArr);
        sketchBytes = offset + itemsRetained * Double.BYTES;
        break;
      }
      case 9: { //Double Compact EMPTY
        if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { memoryValidateThrow(1, preInts); }
        if (serVer != SERIAL_VERSION_EMPTY_FULL) { memoryValidateThrow(2, serVer); }
        layout = Layout.DOUBLE_EMPTY_COMPACT;
        n = 0;
        dyMinK = k;
        numLevels = 1;

        // LEVELS MEM
        levelsArrCompact = Memory.wrap(new int[] {k, k});
        // MIN/MAX MEM
        minMaxArrCompact = Memory.wrap(new double[] {Double.NaN, Double.NaN});
        // ITEMS MEM
        capacityItems = k;
        itemsRetained = 0;
        itemsArrCompact = Memory.wrap(new double[k]);
        sketchBytes = DATA_START_ADR_SINGLE_ITEM; //also used for empty
        itemsArrStart = DATA_START_ADR_SINGLE_ITEM;
        break;
      }
      case 12: { //Double Compact SINGLE
        if (preInts != PREAMBLE_INTS_EMPTY_SINGLE) { memoryValidateThrow(1, preInts); }
        if (serVer != SERIAL_VERSION_SINGLE) { memoryValidateThrow(4, serVer); }
        layout = Layout.DOUBLE_SINGLE_COMPACT;
        n = 1;
        dyMinK = k;
        numLevels = 1;

        // LEVELS MEM
        levelsArrCompact = Memory.wrap(new int[] {k - 1, k});
        final double minMax = srcMem.getDouble(DATA_START_ADR_SINGLE_ITEM);
        // MIN/MAX MEM
        minMaxArrCompact = Memory.wrap(new double[] {minMax, minMax});
        // ITEMS MEM
        capacityItems = k;
        itemsRetained = 1;
        final double[] myDoubleItems = new double[k];
        myDoubleItems[k - 1] = minMax;
        itemsArrCompact = Memory.wrap(myDoubleItems);
        sketchBytes = DATA_START_ADR_SINGLE_ITEM + Double.BYTES;
        itemsArrStart = DATA_START_ADR_SINGLE_ITEM;
        break;
      }
      default: break; //can't happen
    }
  }

  void updatableMemoryValidate(final WritableMemory wSrcMem) {
    if (doublesSketch) { //Double Updatable FULL
      if (preInts != PREAMBLE_INTS_DOUBLE) { memoryValidateThrow(5, preInts); }
      layout = Layout.DOUBLE_UPDATABLE;
      n = extractN(wSrcMem);
      empty = n == 0;       //empty & singleItem are set for convenience
      singleItem = n == 1;  // there is no error checking on these bits
      dyMinK = extractDyMinK(wSrcMem);
      numLevels = extractNumLevels(wSrcMem);

      int offset = DATA_START_ADR_DOUBLE;
      //LEVELS
      levelsArrUpdatable = wSrcMem.writableRegion(offset, (numLevels + 1L) * Integer.BYTES);
      offset += (int)levelsArrUpdatable.getCapacity();
      //MIN/MAX
      minMaxArrUpdatable = wSrcMem.writableRegion(offset, 2L * Double.BYTES);
      offset += (int)minMaxArrUpdatable.getCapacity();
      //ITEMS
      capacityItems = levelsArrUpdatable.getInt((long)numLevels * Integer.BYTES);
      final int itemsArrBytes = capacityItems * Double.BYTES;
      itemsArrStart = offset;
      itemsArrStart = memCapacity - itemsArrBytes;
      if (itemsArrStart < offset) { memoryValidateThrow(24, offset - itemsArrStart); }
      itemsArrUpdatable = wSrcMem.writableRegion(itemsArrStart, itemsArrBytes);
      sketchBytes = itemsArrStart + itemsArrBytes;
    }
    else { //Float Updatable FULL
      if (preInts != PREAMBLE_INTS_FLOAT) { memoryValidateThrow(6, preInts); }
      layout = Layout.FLOAT_UPDATABLE;
      n = extractN(wSrcMem);
      empty = n == 0;       //empty & singleItem are set for convenience
      singleItem = n == 1;  // there is no error checking on these bits
      dyMinK = extractDyMinK(wSrcMem);
      numLevels = extractNumLevels(wSrcMem);
      int offset = DATA_START_ADR_FLOAT;
      //LEVELS
      levelsArrUpdatable = wSrcMem.writableRegion(offset, (numLevels + 1L) * Integer.BYTES);
      offset += (int)levelsArrUpdatable.getCapacity();
      //MIN/MAX
      minMaxArrUpdatable = wSrcMem.writableRegion(offset, 2L * Float.BYTES);
      offset += (int)minMaxArrUpdatable.getCapacity();
      //ITEMS
      capacityItems = levelsArrUpdatable.getInt((long)numLevels * Integer.BYTES);
      final int itemsArrBytes = capacityItems * Float.BYTES;
      itemsArrStart = offset;
      itemsArrStart = memCapacity - itemsArrBytes;
      if (itemsArrStart < offset) { memoryValidateThrow(24, offset - itemsArrStart); }
      itemsArrUpdatable = wSrcMem.writableRegion(itemsArrStart, itemsArrBytes);
      sketchBytes = itemsArrStart + itemsArrBytes;
    }
  }

  private static void memoryValidateThrow(final int errNo, final int value) {
    String msg = "";
    switch (errNo) {
      case 0: msg = "FamilyID Field must be: " + Family.KLL.getID() + ", NOT: " + value; break;
      case 1: msg = "Empty Bit: 1 -> PreInts: " + PREAMBLE_INTS_EMPTY_SINGLE + ", NOT: " + value; break;
      case 2: msg = "Empty Bit: 1 -> SerVer: " + SERIAL_VERSION_EMPTY_FULL + ", NOT: " + value; break;
      //case 3: msg = "Single Item Bit: 1 -> PreInts: " + PREAMBLE_INTS_EMPTY_SINGLE + ", NOT: " + value; break;
      case 4: msg = "Single Item Bit: 1 -> SerVer: " + SERIAL_VERSION_SINGLE + ", NOT: " + value; break;
      case 5: msg = "Double Sketch Bit: 1 -> PreInts: " + PREAMBLE_INTS_DOUBLE + ", NOT: " + value; break;
      case 6: msg = "Double Sketch Bit: 0 -> PreInts: " + PREAMBLE_INTS_FLOAT + ", NOT: " + value; break;
      case 7: msg = "The M field must be set to " + DEFAULT_M + ", NOT: " + value; break;
      //case 8: msg = "The dynamic MinK must be equal to K, NOT: " + value; break;
      //case 9: msg = "numLevels must be one, NOT: " + value; break;
      case 10: msg = "((SerVer == 3) ^ (Updatable Bit)) must = 0."; break;
      case 20: msg = "Empty flag bit and SingleItem flag bit cannot both be set. Flags: " + value; break;
      //case 21: msg = "N != 0 and empty bit is set. N: " + value; break;
      //case 22: msg = "N != 1 and single item bit is set. N: " + value; break;
      //case 23: msg = "Family name is not KLL"; break;
      case 24: msg = "Given Memory has insufficient capacity. Need " + value + " bytes."; break;
      default: msg = "Unknown error: errNo: " + errNo; break;
    }
    throw new SketchesArgumentException(msg);
  }

}

