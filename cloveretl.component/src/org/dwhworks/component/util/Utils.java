package org.dwhworks.component.util;

import org.apache.commons.codec.digest.DigestUtils;

import java.util.Arrays;
import java.util.LinkedList;

/**
 * Some utility functions.
 *
 * @author Nikita Skotnikov
 * @since 26.03.2018
 */
public class Utils {

  /**
   * Returns a fixed-size linked list backed by the specified array.
   *
   * @param a   the array by which the list will be backed
   * @param <T> the class of the objects in the array
   * @return new list backed by the specified array
   */
  @SafeVarargs
  public static <T> LinkedList<T> toLinkedList(T... a) {
    return new LinkedList<>(Arrays.asList(a));
  }

  /**
   * Calculates the MD5 digest and returns the value as a 32 character hex string.
   *
   * @param s a string data for digest
   * @return MD5 digest as a hex string
   */
  public static String md5(String s) {
    return DigestUtils.md5Hex(s.getBytes());
  }

  /**
   * Return <code>true</code> if <code>s</code> is null or has length equal zero.
   *
   * @param s - a string
   * @return true or false value
   */
  public static boolean isEmpty(String s) {
    return s == null || s.isEmpty();
  }


}
