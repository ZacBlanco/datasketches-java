/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.fun;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.tuple.SketchIterator;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSketch;
import com.yahoo.sketches.tuple.strings.ArrayOfStringsSummary;


/**
 * @author Lee Rhodes
 */
public class FunSketchTest {
  private static final String LS = System.getProperty("line.separator");

  @Test
  public void checkFunSketch() {
    final int lgK = 14;
    final FunSketch sketch = new FunSketch(lgK);

    final String[] nodesArr = {"abc", "def" };
    sketch.update(nodesArr);

    final SketchIterator<ArrayOfStringsSummary> it = sketch.iterator();
    int count = 0;
    while (it.next()) {
      final String[] nodesArr2 = it.getSummary().getValue();
      assertEquals(nodesArr2, nodesArr);
      count++;
    }
    assertEquals(count, 1);

    //serialize
    final byte[] byteArr = sketch.toByteArray();
    //deserialize
    Memory mem = Memory.wrap(byteArr);
    ArrayOfStringsSketch aosSketch = new ArrayOfStringsSketch(mem);
    FunSketch sketch2 = new FunSketch(aosSketch);

    //check output
    final SketchIterator<ArrayOfStringsSummary> it2 = sketch2.iterator();
    int count2 = 0;
    while (it2.next()) {
      final String[] nodesArr2 = it2.getSummary().getValue();
      assertEquals(nodesArr2, nodesArr);
      count2++;
    }
    assertEquals(count, count2);
  }

  @Test
  public void checkLgKcompute() {
    final int lgK = FunSketch.computeLgK(.02, .05); //thresh, RSE
    println("LgK: " + lgK);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    System.out.print(s + LS);
  }

  /**
   * @param s value to print
   */
  static void print(String s) {
    System.out.print(s);  //disable here
  }

}