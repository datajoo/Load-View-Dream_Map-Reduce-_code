package com.company;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CategoryCodeTaggedGroupKeyPartitioner extends Partitioner<CategoryCodeTaggedKey, Text> {

  @Override
  public int getPartition(CategoryCodeTaggedKey key, Text val, int numPartitions) {
    // 항공사 코드의 해시값으로 파티션 계산
    int hash = key.getcategorycode().hashCode();
    int partition = hash % numPartitions;
    return partition;
  }
}