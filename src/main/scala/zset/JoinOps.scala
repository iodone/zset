package com.example.zset

/**
 * 字段提取器，用于从键中提取用于比较的值
 */
  case class FieldExtractor[Data, Field](extractor: Data => Field) {
  def apply(data: Data): Field = extractor(data)

  /**
   * 重载 == 操作符，创建等值连接条件
   */
  def ==[OtherData](
      other: FieldExtractor[OtherData, Field]
  ): EquiJoinCondition[Data, OtherData, Field] =
    EquiJoinCondition(this.extractor, other.extractor)
}

/**
 * Join条件的抽象表示
 */
sealed trait JoinCondition[Data1, Data2] {
  def matches(data1: Data1, data2: Data2): Boolean
}

/**
 * 等值连接条件
 */
case class EquiJoinCondition[Data1, Data2, K](
    leftKeyExtractor: Data1 => K,
    rightKeyExtractor: Data2 => K
) extends JoinCondition[Data1, Data2] {
  def matches(data1: Data1, data2: Data2): Boolean =
    leftKeyExtractor(data1) == rightKeyExtractor(data2)
}
