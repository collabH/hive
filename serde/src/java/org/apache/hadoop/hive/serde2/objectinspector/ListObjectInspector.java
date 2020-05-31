/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.objectinspector;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;

import java.util.List;

/**
 * ListObjectInspector.
 *集合对象检查器
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ListObjectInspector extends ObjectInspector {

  // ** Methods that does not need a data object **
  ObjectInspector getListElementObjectInspector();

  // ** Methods that need a data object **
  /**
   * returns null for null list, out-of-the-range index.
   */
  Object getListElement(Object data, int index);

   /**
    * 得到集合长度，如果为-1代表为null
   * returns -1 for data = null.
   */
  int getListLength(Object data);

  /**
   * returns null for data = null.
   *
   * Note: This method should not return a List object that is reused by the
   * same ListObjectInspector, because it's possible that the same
   * ListObjectInspector will be used in multiple places in the code.
   * 
   * However it's OK if the List object is part of the Object data.
   */
  List<?> getList(Object data);

}
