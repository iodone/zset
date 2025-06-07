package com.example.zset

import munit.FunSuite
import com.example.zset.{IndexedZDataset, WeightType, ZSet, ZDataset}
import WeightType.{IntegerWeight, LongWeight, given}
import com.example.zset.ZDatasetOps.*
import com.example.zset.IndexedZDataset.*

class ZDatasetGroupSpec extends FunSuite {

  // 测试数据类型定义
  case class Employee(name: String, department: String, salary: Double, age: Int)
  case class Sale(id: Int, product: String, amount: Double, region: String)
  case class Student(name: String, grade: String, score: Int, subject: String)

  // === 基础 GroupBy + IndexedZDataset 转换测试 ===
  test("groupBy 直接作为 IndexedZDataset") {
    val employees = ZDataset.fromIntIterable(
      List(
        Employee("Alice", "Engineering", 80000, 25),
        Employee("Bob", "Engineering", 85000, 30),
        Employee("Charlie", "Sales", 60000, 28),
        Employee("David", "Sales", 65000, 32)
      )
    )

    // 按部门分组，直接得到 IndexedZDataset[String, Employee, Int]
    val groupedByDept: IndexedZDataset[String, Employee, Int] = employees.groupBy(_.department)

    // 验证基本结构
    assertEquals(groupedByDept.keyCount, 2)
    assert(groupedByDept.containsKey("Engineering"))
    assert(groupedByDept.containsKey("Sales"))

    // 验证每个部门的员工数量
    val engineeringCount = groupedByDept.getZSet("Engineering").underlying.entryCount
    val salesCount       = groupedByDept.getZSet("Sales").underlying.entryCount
    assertEquals(engineeringCount, 2)
    assertEquals(salesCount, 2)
  }

  // === IndexedZDataset 链式聚合操作测试 ===
  test("IndexedZDataset 链式聚合操作 - sum") {
    val employees = ZDataset.fromIntIterable(
      List(
        Employee("Alice", "Engineering", 80000, 25),
        Employee("Bob", "Engineering", 85000, 30),
        Employee("Charlie", "Sales", 60000, 28),
        Employee("David", "Sales", 65000, 32)
      )
    )

    // 链式操作：按部门分组 -> 汇总薪资
    val salaryByDept = employees
      .groupBy(_.department)
      .sum(_.salary)

    // 验证结果
    assertEquals(salaryByDept.keyCount, 2)

    val engineeringSalary = salaryByDept.getZSet("Engineering").underlying.entries.head._1
    val salesSalary       = salaryByDept.getZSet("Sales").underlying.entries.head._1

    assertEquals(engineeringSalary, 165000.0) // 80000 + 85000
    assertEquals(salesSalary, 125000.0)       // 60000 + 65000
  }

  test("IndexedZDataset 链式聚合操作 - count") {
    val students = ZDataset.fromIntIterable(
      List(
        Student("Alice", "A", 95, "Math"),
        Student("Bob", "A", 88, "Math"),
        Student("Charlie", "B", 92, "Math"),
        Student("David", "B", 85, "Math"),
        Student("Eve", "A", 90, "Science")
      )
    )

    // 链式操作：按年级分组 -> 统计学生数量
    val countByGrade = students
      .groupBy(_.grade)
      .count

    assertEquals(countByGrade.keyCount, 2)

    val gradeACount = countByGrade.getZSet("A").underlying.entries.head._1
    val gradeBCount = countByGrade.getZSet("B").underlying.entries.head._1

    assertEquals(gradeACount, 3) // Alice, Bob, Eve
    assertEquals(gradeBCount, 2) // Charlie, David
  }

  test("IndexedZDataset 链式聚合操作 - avg") {
    val students = ZDataset.fromIntIterable(
      List(
        Student("Alice", "A", 95, "Math"),
        Student("Bob", "A", 85, "Math"),
        Student("Charlie", "B", 90, "Math"),
        Student("David", "B", 80, "Math")
      )
    )

    // 链式操作：按年级分组 -> 计算平均分
    val avgByGrade = students
      .groupBy(_.grade)
      .avg(_.score.toDouble)

    assertEquals(avgByGrade.keyCount, 2)

    val gradeAAvg = avgByGrade.getZSet("A").underlying.entries.head._1
    val gradeBAvg = avgByGrade.getZSet("B").underlying.entries.head._1

    assertEquals(gradeAAvg, Some(90.0)) // (95 + 85) / 2
    assertEquals(gradeBAvg, Some(85.0)) // (90 + 80) / 2
  }

  test("IndexedZDataset 链式聚合操作 - max/min") {
    val sales = ZDataset.fromIntIterable(
      List(
        Sale(1, "Laptop", 1200, "North"),
        Sale(2, "Mouse", 25, "North"),
        Sale(3, "Keyboard", 80, "South"),
        Sale(4, "Monitor", 300, "South"),
        Sale(5, "Tablet", 500, "North")
      )
    )

    // 链式操作：按地区分组 -> 计算最大和最小销售金额
    val maxByRegion = sales
      .groupBy(_.region)
      .max(_.amount)

    val minByRegion = sales
      .groupBy(_.region)
      .min(_.amount)

    assertEquals(maxByRegion.keyCount, 2)
    assertEquals(minByRegion.keyCount, 2)

    val northMax = maxByRegion.getZSet("North").underlying.entries.head._1
    val southMax = maxByRegion.getZSet("South").underlying.entries.head._1
    val northMin = minByRegion.getZSet("North").underlying.entries.head._1
    val southMin = minByRegion.getZSet("South").underlying.entries.head._1

    assertEquals(northMax, Some(1200.0)) // Laptop
    assertEquals(southMax, Some(300.0))  // Monitor
    assertEquals(northMin, Some(25.0))   // Mouse
    assertEquals(southMin, Some(80.0))   // Keyboard
  }

  // === Flatten 操作测试 ===
  test("链式操作：groupBy -> agg -> flatten") {
    val employees = ZDataset.fromIntIterable(
      List(
        Employee("Alice", "Engineering", 80000, 25),
        Employee("Bob", "Engineering", 85000, 30),
        Employee("Charlie", "Sales", 60000, 28),
        Employee("David", "Sales", 65000, 32)
      )
    )

    // 链式操作：按部门分组 -> 计算平均薪资 -> flatten 为 ZDataset
    val flattenedAvg = employees
      .groupBy(_.department)
      .avg(_.salary)
      .flatten { (dept, avgSalary) =>
        (dept, avgSalary.getOrElse(0.0))
      }

    // 验证flatten结果
    assertEquals(flattenedAvg.underlying.entryCount, 2)

    val entries                        = flattenedAvg.underlying.entries.toList.sortBy(_._1._1)
    val (engineeringEntry, salesEntry) = (entries(0)._1, entries(1)._1)

    assertEquals(engineeringEntry._1, "Engineering")
    assertEquals(engineeringEntry._2, 82500.0) // (80000 + 85000) / 2
    assertEquals(salesEntry._1, "Sales")
    assertEquals(salesEntry._2, 62500.0) // (60000 + 65000) / 2
  }

  test("链式操作：groupBy -> agg -> flatten -> ZDataset操作") {
    val sales = ZDataset.fromIntIterable(
      List(
        Sale(1, "Laptop", 1200, "North"),
        Sale(2, "Mouse", 25, "North"),
        Sale(3, "Keyboard", 80, "South"),
        Sale(4, "Monitor", 300, "South"),
        Sale(5, "Tablet", 500, "East"),
        Sale(6, "Phone", 800, "East")
      )
    )

    // 链式操作：按地区分组 -> 计算总销售额 -> flatten -> 筛选高绩效地区 -> 计算平均值
    val avgOfHighPerforming = sales
      .groupBy(_.region)
      .sum(_.amount)
      .flatten((region, total) => (region, total))
      .where(_._2 > 500.0)
      .avg(_._2)

    // 验证结果
    assertEquals(avgOfHighPerforming, Some(1262.5)) // (1225 + 1300) / 2
  }

  // === 复杂链式场景测试 ===
  test("复杂链式操作：多次 groupBy + agg + flatten") {
    val students = ZDataset.fromIntIterable(
      List(
        Student("Alice", "A", 95, "Math"),
        Student("Bob", "A", 85, "Math"),
        Student("Charlie", "A", 90, "Science"),
        Student("David", "B", 88, "Math"),
        Student("Eve", "B", 92, "Science"),
        Student("Frank", "C", 78, "Math"),
        Student("Grace", "C", 85, "Science")
      )
    )

    // 第一阶段：按年级分组 -> 计算平均分 -> flatten -> 筛选高绩效年级
    val highPerformingGrades = students
      .groupBy(_.grade)
      .avg(_.score.toDouble)
      .flatten((grade, avg) => (grade, avg.getOrElse(0.0)))
      .where(_._2 > 85.0)

    // 验证第一阶段结果
    assertEquals(highPerformingGrades.underlying.entryCount, 2)
    val gradeNames = highPerformingGrades.underlying.entries.map(_._1._1).toSet
    assert(gradeNames.contains("A")) // (95 + 85 + 90) / 3 = 90
    assert(gradeNames.contains("B")) // (88 + 92) / 2 = 90

    // 第二阶段：筛选高绩效年级的学生 -> 按学科分组 -> 计算最高分 -> flatten
    val maxFlattened = students
      .where(s => gradeNames.contains(s.grade))
      .groupBy(_.subject)
      .max(_.score)
      .flatten((subject, maxScore) => (subject, maxScore.getOrElse(0)))

    // 验证最终结果
    assertEquals(maxFlattened.underlying.entryCount, 2)
    val subjectMaxes = maxFlattened.underlying.entries.toMap
    assertEquals(subjectMaxes(("Math", 95)), 1)    // Alice的95分（A年级）
    assertEquals(subjectMaxes(("Science", 92)), 1) // Eve的92分（B年级）
  }

  test("权重处理：链式操作 - 带权重的 groupBy 和聚合") {
    // 创建带权重的销售数据
    val weightedSales = ZDataset.fromPairs(
      List(
        (Sale(1, "Laptop", 1200, "North"), 2), // 权重为2，表示卖了2台
        (Sale(2, "Mouse", 25, "North"), 5),    // 权重为5，表示卖了5个
        (Sale(3, "Keyboard", 80, "South"), 3), // 权重为3，表示卖了3个
        (Sale(4, "Monitor", 300, "South"), 1)  // 权重为1，表示卖了1台
      )
    )

    // 链式操作：按地区分组 -> 计算加权总销售额 -> flatten -> 计算总体平均
    val overallWeightedAvg = weightedSales
      .groupBy(_.region)
      .sum(_.amount)
      .flatten((region, total) => (region, total))
      .avg(_._2)

    // 验证加权计算结果
    assertEquals(overallWeightedAvg, Some(1532.5)) // (2525 + 540) / 2 = 1532.5
  }

  test("超长链式操作：groupBy -> agg -> flatten -> groupBy -> agg -> flatten") {
    val employees = ZDataset.fromIntIterable(
      List(
        Employee("Alice", "Engineering", 80000, 25),
        Employee("Bob", "Engineering", 85000, 30),
        Employee("Charlie", "Sales", 60000, 28),
        Employee("David", "Sales", 65000, 32),
        Employee("Eve", "Marketing", 70000, 26),
        Employee("Frank", "Marketing", 75000, 29)
      )
    )

    // 超长链式操作
    val finalResult = employees
      .groupBy(_.department)    // 按部门分组
      .avg(_.salary)            // 计算平均薪资
      .flatten { (dept, avg) => // flatten 为 ZDataset
        val avgSalary   = avg.getOrElse(0.0)
        val salaryLevel = if (avgSalary > 75000) "High" else "Medium"
        (dept, avgSalary, salaryLevel)
      }
      .groupBy(_._3)                             // 按薪资水平重新分组
      .count                                     // 计算各薪资水平的部门数量
      .flatten((level, count) => (level, count)) // 最终 flatten

    // 验证结果
    assertEquals(finalResult.underlying.entryCount, 2)
    val levelCounts = finalResult.underlying.entries.toMap
    assertEquals(levelCounts(("High", 1)), 1)   // Engineering (82500)
    assertEquals(levelCounts(("Medium", 2)), 1) // Sales (62500), Marketing (72500)
  }

  test("混合聚合链式操作") {
    val sales = ZDataset.fromIntIterable(
      List(
        Sale(1, "Laptop", 1200, "North"),
        Sale(2, "Mouse", 25, "North"),
        Sale(3, "Keyboard", 80, "South"),
        Sale(4, "Monitor", 300, "South"),
        Sale(5, "Tablet", 500, "West"),
        Sale(6, "Phone", 800, "West")
      )
    )

    // 混合多种聚合操作的链式调用
    val summaryByRegion = sales
      .groupBy(_.region)
      .aggregate(
        (0.0, 0.0, 0) // (sum, max, count)
      ) { case ((sum, max, count), sale, weight) =>
        (sum + sale.amount, math.max(max, sale.amount), count + 1)
      }
      .flatten { (region, summary) =>
        val (totalSales, maxSale, saleCount) = summary
        val avgSale                          = if (saleCount > 0) totalSales / saleCount else 0.0
        (region, totalSales, maxSale, avgSale, saleCount)
      }

    // 验证混合聚合结果
    assertEquals(summaryByRegion.underlying.entryCount, 3)

    val summaries = summaryByRegion.underlying.entries.toList.sortBy(_._1._1)

    // North: Laptop(1200) + Mouse(25) = 1225, max=1200, count=2, avg=612.5
    val northSummary = summaries.find(_._1._1 == "North").get._1
    assertEquals(northSummary._2, 1225.0) // total
    assertEquals(northSummary._3, 1200.0) // max
    assertEquals(northSummary._4, 612.5)  // avg
    assertEquals(northSummary._5, 2)      // count
  }

}
