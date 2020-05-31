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

package org.apache.hadoop.hive.ql.udf.generic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

/**
 * 通用的udaf收集器
 */
public class GenericUDAFMkCollectionEvaluator extends GenericUDAFEvaluator
    implements Serializable {

  private static final long serialVersionUID = 1l;

  enum BufferType { SET, LIST }

  //输入对象
  // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
  private transient ObjectInspector inputOI;
  // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
  // of objs)
  private transient StandardListObjectInspector loi;

  //内部合并数据结构
  private transient ListObjectInspector internalMergeOI;

  //转换类型，支持set和list对应collect_set、collet_list
  private BufferType bufferType;

  //needed by kyro
  public GenericUDAFMkCollectionEvaluator() {
  }

  public GenericUDAFMkCollectionEvaluator(BufferType bufferType){
    this.bufferType = bufferType;
  }

  /**
   * hive调用初始化一个UDAF evaluator类
   * @param m
   *          The mode of aggregation.
   * @param parameters
   *          The ObjectInspector for the parameters: In PARTIAL1 and COMPLETE
   *          mode, the parameters are original data; In PARTIAL2 and FINAL
   *          mode, the parameters are just partial aggregations (in that case,
   *          the array will always have a single element).
   * @return
   * @throws HiveException
   */
  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters)
      throws HiveException {
    super.init(m, parameters);
    // init output object inspectors
    // The output of a partial aggregation is a list
    if (m == Mode.PARTIAL1) {
      inputOI = parameters[0];
      return ObjectInspectorFactory.getStandardListObjectInspector(
          ObjectInspectorUtils.getStandardObjectInspector(inputOI));
    } else {
      if (!(parameters[0] instanceof ListObjectInspector)) {
        //no map aggregation.
        inputOI = ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
        return ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
      } else {
        internalMergeOI = (ListObjectInspector) parameters[0];
        inputOI = internalMergeOI.getListElementObjectInspector();
        loi = (StandardListObjectInspector)
            ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
        return loi;
      }
    }
  }


  /**
   * 数据聚合缓存
   */
  class MkArrayAggregationBuffer extends AbstractAggregationBuffer {

    //输出数据
    private Collection<Object> container;

    public MkArrayAggregationBuffer() {
      if (bufferType == BufferType.LIST){
        container = new ArrayList<Object>();
      } else if(bufferType == BufferType.SET){
        container = new LinkedHashSet<Object>();
      } else {
        throw new RuntimeException("Buffer type unknown");
      }
    }
  }

  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((MkArrayAggregationBuffer) agg).container.clear();
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException {
    MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
    return ret;
  }

  //mapside
  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters)
      throws HiveException {
    assert (parameters.length == 1);
    //得到传入参数
    Object p = parameters[0];

    if (p != null) {
      MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
      //数据迭代塞入集合容器
      putIntoCollection(p, myagg);
    }
  }

  //mapside，提取一部分数据塞入新的list中
  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
    List<Object> ret = new ArrayList<Object>(myagg.container.size());
    ret.addAll(myagg.container);
    return ret;
  }

  @Override
  public void merge(AggregationBuffer agg, Object partial)
      throws HiveException {
    MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
    List<Object> partialResult = (List<Object>) internalMergeOI.getList(partial);
    if (partialResult != null) {
      //便利数据加入集合中
      for(Object i : partialResult) {
        putIntoCollection(i, myagg);
      }
    }
  }

  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
    List<Object> ret = new ArrayList<Object>(myagg.container.size());
    ret.addAll(myagg.container);
    return ret;
  }

  private void putIntoCollection(Object p, MkArrayAggregationBuffer myagg) {
    Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,  this.inputOI);
    myagg.container.add(pCopy);
  }

  public BufferType getBufferType() {
    return bufferType;
  }

  public void setBufferType(BufferType bufferType) {
    this.bufferType = bufferType;
  }

}
