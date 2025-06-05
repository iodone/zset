package com.example.zset

import munit.FunSuite
import com.example.zset.{WeightType, ZSet, ZSetDB}
import WeightType.{IntegerWeight, LongWeight, given}
import com.example.zset.ZSetDBOps.*

class ZSetDBSpec extends FunSuite {

  // 测试数据类型定义
  case class Person(name: String, age: Int)
  case class Order(id: Int, customerId: Int, amount: Double)
  case class Product(id: Int, name: String, price: Double)

  // === 基础构造测试 ===
  test("创建空的ZSetDB") {
    val db = ZSetDB.empty[String, Int]
    assertEquals(db.underlying.entryCount, 0)
    assert(db.underlying.isEmpty)
  }

  test("从可迭代对象创建ZSetDB") {
    val db = ZSetDB.fromIntIterable(List("a", "b", "a", "c"))
    assertEquals(db.underlying.getWeight("a"), 2)
    assertEquals(db.underlying.getWeight("b"), 1)
    assertEquals(db.underlying.getWeight("c"), 1)
    assertEquals(db.underlying.entryCount, 3)
  }

  test("从权重对创建ZSetDB") {
    val db = ZSetDB.fromPairs(List(("x", 2), ("y", 3), ("x", 1)))
    assertEquals(db.underlying.getWeight("x"), 3)
    assertEquals(db.underlying.getWeight("y"), 3)
    assertEquals(db.underlying.entryCount, 2)
  }

  test("创建单元素ZSetDB") {
    val db = ZSetDB.single("hello", 5)
    assertEquals(db.underlying.getWeight("hello"), 5)
    assertEquals(db.underlying.entryCount, 1)
  }

  test("便捷构造方法 - apply") {
    val db = ZSetDB("a", "b", "c")
    assertEquals(db.underlying.getWeight("a"), 1)
    assertEquals(db.underlying.getWeight("b"), 1)
    assertEquals(db.underlying.getWeight("c"), 1)
  }

  test("便捷构造方法 - weighted") {
    val db = ZSetDB.weighted(("a", 2), ("b", 3))
    assertEquals(db.underlying.getWeight("a"), 2)
    assertEquals(db.underlying.getWeight("b"), 3)
  }

  // === SELECT 操作测试 ===
  test("select - 基本投影") {
    val persons = ZSetDB.fromIntIterable(
      List(
        Person("Alice", 25),
        Person("Bob", 30),
        Person("Charlie", 25)
      )
    )

    val ages = persons.select(_.age)
    assertEquals(ages.underlying.getWeight(25), 2)
    assertEquals(ages.underlying.getWeight(30), 1)
    assertEquals(ages.underlying.entryCount, 2)
  }

  test("select - 字符串长度投影") {
    val words   = ZSetDB.fromIntIterable(List("hello", "world", "scala"))
    val lengths = words.select(_.length)

    assertEquals(lengths.underlying.getWeight(5), 3) // "hello" and "world"
    assertEquals(lengths.underlying.entryCount, 1)
  }

  test("select - 复杂对象投影") {
    val orders = ZSetDB.fromIntIterable(
      List(
        Order(1, 100, 50.0),
        Order(2, 101, 75.0),
        Order(3, 100, 25.0)
      )
    )

    val customerIds = orders.select(_.customerId)
    assertEquals(customerIds.underlying.getWeight(100), 2)
    assertEquals(customerIds.underlying.getWeight(101), 1)
  }

  // === WHERE 操作测试 ===
  test("where - 基本过滤") {
    val numbers = ZSetDB.fromIntIterable(List(1, 2, 3, 4, 5, 6))
    val evens   = numbers.where(_ % 2 == 0)

    assertEquals(evens.underlying.getWeight(2), 1)
    assertEquals(evens.underlying.getWeight(4), 1)
    assertEquals(evens.underlying.getWeight(6), 1)
    assertEquals(evens.underlying.entryCount, 3)
    assert(!evens.underlying.contains(1))
    assert(!evens.underlying.contains(3))
    assert(!evens.underlying.contains(5))
  }

  test("where - 复杂条件过滤") {
    val persons = ZSetDB.fromIntIterable(
      List(
        Person("Alice", 25),
        Person("Bob", 30),
        Person("Charlie", 22),
        Person("David", 35)
      )
    )

    val adults = persons.where(_.age >= 25)
    assertEquals(adults.underlying.entryCount, 3)
    assert(adults.underlying.contains(Person("Alice", 25)))
    assert(adults.underlying.contains(Person("Bob", 30)))
    assert(adults.underlying.contains(Person("David", 35)))
    assert(!adults.underlying.contains(Person("Charlie", 22)))
  }

  test("where - 字符串模式过滤") {
    val words       = ZSetDB.fromIntIterable(List("apple", "banana", "apricot", "orange"))
    val startsWithA = words.where(_.startsWith("a"))

    assertEquals(startsWithA.underlying.entryCount, 2)
    assert(startsWithA.underlying.contains("apple"))
    assert(startsWithA.underlying.contains("apricot"))
    assert(!startsWithA.underlying.contains("banana"))
    assert(!startsWithA.underlying.contains("orange"))
  }

  // === UNION 操作测试 ===
  test("union - 基本并集") {
    val db1 = ZSetDB.fromIntIterable(List("a", "b", "c"))
    val db2 = ZSetDB.fromIntIterable(List("c", "d", "e"))

    val result = db1.union(db2)
    assertEquals(result.underlying.getWeight("a"), 1)
    assertEquals(result.underlying.getWeight("b"), 1)
    assertEquals(result.underlying.getWeight("c"), 1) // distinct 去重
    assertEquals(result.underlying.getWeight("d"), 1)
    assertEquals(result.underlying.getWeight("e"), 1)
    assertEquals(result.underlying.entryCount, 5)
  }

  test("union - 带权重并集") {
    val db1 = ZSetDB.fromPairs(List(("a", 2), ("b", 3)))
    val db2 = ZSetDB.fromPairs(List(("a", 1), ("c", 4)))

    val result = db1.union(db2)
    assertEquals(result.underlying.getWeight("a"), 1) // distinct 处理
    assertEquals(result.underlying.getWeight("b"), 1)
    assertEquals(result.underlying.getWeight("c"), 1)
  }

  test("union - 空集合并集") {
    val db1 = ZSetDB.fromIntIterable(List("a", "b"))
    val db2 = ZSetDB.empty[String, Int]

    val result = db1.union(db2)
    assertEquals(result.underlying.entryCount, 2)
    assertEquals(result.underlying.getWeight("a"), 1)
    assertEquals(result.underlying.getWeight("b"), 1)
  }

  // === INTERSECT 操作测试 ===
  test("intersect - 基本交集") {
    val db1 = ZSetDB.fromIntIterable(List("a", "b", "c"))
    val db2 = ZSetDB.fromIntIterable(List("b", "c", "d"))

    val result = db1.intersect(db2)
    assertEquals(result.underlying.entryCount, 2)
    assert(result.underlying.contains("b"))
    assert(result.underlying.contains("c"))
    assert(!result.underlying.contains("a"))
    assert(!result.underlying.contains("d"))
  }

  test("intersect - 带权重交集") {
    val db1 = ZSetDB.fromPairs(List(("a", 2), ("b", 3), ("c", 1)))
    val db2 = ZSetDB.fromPairs(List(("b", 5), ("c", 2), ("d", 1)))

    val result = db1.intersect(db2)
    assertEquals(result.underlying.entryCount, 2)
    assertEquals(result.underlying.getWeight("b"), 3) // 保持左侧权重
    assertEquals(result.underlying.getWeight("c"), 1) // 保持左侧权重
  }

  test("intersect - 空集合交集") {
    val db1 = ZSetDB.fromIntIterable(List("a", "b"))
    val db2 = ZSetDB.empty[String, Int]

    val result = db1.intersect(db2)
    assertEquals(result.underlying.entryCount, 0)
    assert(result.underlying.isEmpty)
  }

  // === EXCEPT 操作测试 ===
  test("except - 基本差集") {
    val db1 = ZSetDB.fromIntIterable(List("a", "b", "c", "d"))
    val db2 = ZSetDB.fromIntIterable(List("b", "c"))

    val result = db1.except(db2)
    assertEquals(result.underlying.entryCount, 2)
    assert(result.underlying.contains("a"))
    assert(result.underlying.contains("d"))
    assert(!result.underlying.contains("b"))
    assert(!result.underlying.contains("c"))
  }

  test("except - 带权重差集") {
    val db1 = ZSetDB.fromPairs(List(("a", 2), ("b", 3), ("c", 1)))
    val db2 = ZSetDB.fromPairs(List(("b", 5), ("e", 1)))

    val result = db1.except(db2)
    assertEquals(result.underlying.entryCount, 2)
    assertEquals(result.underlying.getWeight("a"), 1) // distinct 处理
    assertEquals(result.underlying.getWeight("c"), 1) // distinct 处理
    assert(!result.underlying.contains("b"))
  }

  test("except - 空集合差集") {
    val db1 = ZSetDB.fromIntIterable(List("a", "b"))
    val db2 = ZSetDB.empty[String, Int]

    val result = db1.except(db2)
    assertEquals(result.underlying.entryCount, 2)
    assert(result.underlying.contains("a"))
    assert(result.underlying.contains("b"))
  }

  // === JOIN 操作测试 ===
  test("join - 基本内连接") {
    val customers = ZSetDB.fromIntIterable(List((1, "Alice"), (2, "Bob"), (3, "Charlie")))
    val orders = ZSetDB.fromIntIterable(List((1, 100.0), (2, 150.0), (1, 75.0)))

    val result = customers.join(orders)
    assertEquals(result.underlying.entryCount, 3)
    assert(result.underlying.contains((1, "Alice", 100.0)))
    assert(result.underlying.contains((1, "Alice", 75.0)))
    assert(result.underlying.contains((2, "Bob", 150.0)))
    assert(!result.underlying.contains((3, "Charlie", 0.0))) // Charlie 没有订单
  }

  test("join - 复杂对象连接") {
    val employees = ZSetDB.fromIntIterable(List((1, Person("Alice", 25)), (2, Person("Bob", 30))))
    val salaries = ZSetDB.fromIntIterable(List((1, 50000.0), (2, 60000.0)))

    val result = employees.join(salaries)
    assertEquals(result.underlying.entryCount, 2)
    assert(result.underlying.contains((1, Person("Alice", 25), 50000.0)))
    assert(result.underlying.contains((2, Person("Bob", 30), 60000.0)))
  }

  // === AGG 操作测试 ===
  test("agg - 数字求和") {
    val numbers = ZSetDB.fromIntIterable(List(1, 2, 3, 4, 5))
    val sum = numbers.agg(0, (acc, n) => acc + n)
    assertEquals(sum, 15)
  }

  test("agg - 字符串连接") {
    val words = ZSetDB.fromIntIterable(List("Hello", " ", "World"))
    val concatenated = words.agg("", (acc, word) => acc + word)
    assertEquals(concatenated, "Hello World")
  }

  test("agg - 复杂对象聚合") {
    val persons = ZSetDB.fromIntIterable(List(Person("Alice", 25), Person("Bob", 30), Person("Charlie", 35)))
    val totalAge = persons.agg(0, (acc, person) => acc + person.age)
    assertEquals(totalAge, 90)
  }

  // === GROUPBY 操作测试 ===
  test("groupBy - 基本分组") {
    val persons = ZSetDB.fromIntIterable(List(
      Person("Alice", 25),
      Person("Bob", 25),
      Person("Charlie", 30),
      Person("David", 30),
      Person("Eve", 25)
    ))

    val ageGroups = persons.groupBy(_.age)
    assertEquals(ageGroups.underlying.getWeight((25, 3)), 1) // 3个25岁的人
    assertEquals(ageGroups.underlying.getWeight((30, 2)), 1) // 2个30岁的人
    assertEquals(ageGroups.underlying.entryCount, 2)
  }

  test("groupBy - 字符串长度分组") {
    val words = ZSetDB.fromIntIterable(List("cat", "dog", "bird", "fish"))
    val lengthGroups = words.groupBy(_.length)

    assertEquals(lengthGroups.underlying.getWeight((3, 2)), 1) // cat, dog
    assertEquals(lengthGroups.underlying.getWeight((4, 2)), 1) // bird, fish
    assertEquals(lengthGroups.underlying.entryCount, 2)
  }

  // === COUNT 操作测试 ===
  test("count - 基本计数") {
    val words = ZSetDB.fromIntIterable(List("apple", "banana", "cherry"))
    val counts = words.count

    assertEquals(counts.underlying.getWeight(("apple", 1)), 1)
    assertEquals(counts.underlying.getWeight(("banana", 1)), 1)
    assertEquals(counts.underlying.getWeight(("cherry", 1)), 1)
    assertEquals(counts.underlying.entryCount, 3)
  }

  test("count - 重复元素计数") {
    val numbers = ZSetDB.fromPairs(List((1, 3), (2, 2), (1, 1))) // 注意权重会累加
    val counts = numbers.count

    assertEquals(counts.underlying.entryCount, 2) // distinct 元素: 1, 2
    assert(counts.underlying.contains((1, 1)))
    assert(counts.underlying.contains((2, 1)))
  }

  // === SUM 操作测试 ===
  test("sum - 整数求和") {
    val numbers = ZSetDB.fromIntIterable(List(1, 2, 3, 4, 5))
    val total = numbers.sum(identity)
    assertEquals(total, 15)
  }

  test("sum - 带权重求和") {
    val numbers = ZSetDB.fromPairs(List((10, 2), (20, 3))) // 10*2 + 20*3 = 80
    val total = numbers.sum(identity)
    assertEquals(total, 80)
  }

  test("sum - 复杂对象属性求和") {
    val orders = ZSetDB.fromIntIterable(List(
      Order(1, 100, 50.0),
      Order(2, 101, 75.0),
      Order(3, 102, 25.0)
    ))
    val totalAmount = orders.sum(_.amount)
    assertEquals(totalAmount, 150.0)
  }

  // === MAX 操作测试 ===
  test("max - 整数最大值") {
    val numbers = ZSetDB.fromIntIterable(List(1, 5, 3, 9, 2))
    val maximum = numbers.max(identity)
    assertEquals(maximum, Some(9))
  }

  test("max - 复杂对象属性最大值") {
    val persons = ZSetDB.fromIntIterable(List(
      Person("Alice", 25),
      Person("Bob", 30),
      Person("Charlie", 22)
    ))
    val maxAge = persons.max(_.age)
    assertEquals(maxAge, Some(30))
  }

  test("max - 空集合最大值") {
    val empty = ZSetDB.empty[Int, Int]
    val maximum = empty.max(identity)
    assertEquals(maximum, None)
  }

  // === MIN 操作测试 ===
  test("min - 整数最小值") {
    val numbers = ZSetDB.fromIntIterable(List(1, 5, 3, 9, 2))
    val minimum = numbers.min(identity)
    assertEquals(minimum, Some(1))
  }

  test("min - 复杂对象属性最小值") {
    val persons = ZSetDB.fromIntIterable(List(
      Person("Alice", 25),
      Person("Bob", 30),
      Person("Charlie", 22)
    ))
    val minAge = persons.min(_.age)
    assertEquals(minAge, Some(22))
  }

  test("min - 空集合最小值") {
    val empty = ZSetDB.empty[Int, Int]
    val minimum = empty.min(identity)
    assertEquals(minimum, None)
  }

  // === AVG 操作测试 ===
  test("avg - 整数平均值") {
    val numbers = ZSetDB.fromIntIterable(List(1, 2, 3, 4, 5))
    val average = numbers.avg(identity)
    assertEquals(average, Some(3.0))
  }

  test("avg - 带权重平均值") {
    val numbers = ZSetDB.fromPairs(List((10, 2), (20, 3))) // (10*2 + 20*3)/(2+3) = 80/5 = 16
    val average = numbers.avg(identity)
    assertEquals(average, Some(16.0))
  }

  test("avg - 复杂对象属性平均值") {
    val persons = ZSetDB.fromIntIterable(List(
      Person("Alice", 20),
      Person("Bob", 30),
      Person("Charlie", 40)
    ))
    val avgAge = persons.avg(_.age)
    assertEquals(avgAge, Some(30.0))
  }

  test("avg - 空集合平均值") {
    val empty = ZSetDB.empty[Int, Int]
    val average = empty.avg(identity)
    assertEquals(average, None)
  }

  // === DISTINCT 操作测试 ===
  test("distinct - 基本去重") {
    val numbers = ZSetDB.fromPairs(List((1, 3), (2, 2), (1, 1))) // 权重会累加，然后distinct
    val unique = numbers.distinct

    assertEquals(unique.underlying.entryCount, 2)
    assertEquals(unique.underlying.getWeight(1), 1) // distinct 后权重为1
    assertEquals(unique.underlying.getWeight(2), 1) // distinct 后权重为1
  }

  test("distinct - 复杂对象去重") {
    val persons = ZSetDB.fromPairs(List(
      (Person("Alice", 25), 2),
      (Person("Bob", 30), 1),
      (Person("Alice", 25), 1)
    ))
    val unique = persons.distinct

    assertEquals(unique.underlying.entryCount, 2)
    assertEquals(unique.underlying.getWeight(Person("Alice", 25)), 1)
    assertEquals(unique.underlying.getWeight(Person("Bob", 30)), 1)
  }

  // === CARTESIAN PRODUCT 操作测试 ===
  test("cartesianProduct - 基本笛卡尔积") {
    val colors = ZSetDB.fromIntIterable(List("red", "blue"))
    val sizes = ZSetDB.fromIntIterable(List("S", "M"))

    val products = colors.cartesianProduct(sizes, (color, size) => s"$color-$size")
    assertEquals(products.underlying.entryCount, 4)
    assert(products.underlying.contains("red-S"))
    assert(products.underlying.contains("red-M"))
    assert(products.underlying.contains("blue-S"))
    assert(products.underlying.contains("blue-M"))
  }

  test("cartesianProduct - 数字组合") {
    val nums1 = ZSetDB.fromIntIterable(List(1, 2))
    val nums2 = ZSetDB.fromIntIterable(List(10, 20))

    val sums = nums1.cartesianProduct(nums2, (a, b) => a + b)
    assertEquals(sums.underlying.entryCount, 4)
    assert(sums.underlying.contains(11)) // 1 + 10
    assert(sums.underlying.contains(21)) // 1 + 20
    assert(sums.underlying.contains(12)) // 2 + 10
    assert(sums.underlying.contains(22)) // 2 + 20
  }

  // === SORTBY 操作测试 ===
  test("sortBy - 基本排序") {
    val numbers = ZSetDB.fromIntIterable(List(3, 1, 4, 5)) // 移除重复元素，因为ZSetDB会去重
    val sorted = numbers.sortBy(identity)

    assertEquals(sorted, List(1, 3, 4, 5))
  }

  test("sortBy - 复杂对象排序") {
    val persons = ZSetDB.fromIntIterable(List(
      Person("Charlie", 22),
      Person("Alice", 25),
      Person("Bob", 30)
    ))
    val sortedByAge = persons.sortBy(_.age)

    assertEquals(sortedByAge, List(
      Person("Charlie", 22),
      Person("Alice", 25),
      Person("Bob", 30)
    ))
  }

  test("sortBy - 字符串长度排序") {
    val words = ZSetDB.fromIntIterable(List("banana", "cat", "elephant"))
    val sortedByLength = words.sortBy(_.length)

    assertEquals(sortedByLength, List("cat", "banana", "elephant"))
  }

  // === TAKE 操作测试 ===
  test("take - 基本取前N项") {
    val numbers = ZSetDB.fromIntIterable(List(1, 2, 3, 4, 5))
    val first3 = numbers.take(3)

    assertEquals(first3.underlying.entryCount, 3)
    // 注意：take的结果顺序可能不确定，只验证数量和包含关系
    assert(first3.underlying.entryCount <= 3)
  }

  test("take - 取超过总数的项") {
    val numbers = ZSetDB.fromIntIterable(List(1, 2, 3))
    val first10 = numbers.take(10)

    assertEquals(first10.underlying.entryCount, 3) // 应该返回所有元素
    assert(first10.underlying.contains(1))
    assert(first10.underlying.contains(2))
    assert(first10.underlying.contains(3))
  }

  test("take - 取0项") {
    val numbers = ZSetDB.fromIntIterable(List(1, 2, 3))
    val first0 = numbers.take(0)

    assertEquals(first0.underlying.entryCount, 0)
    assert(first0.underlying.isEmpty)
  }

  // === 综合测试 ===
  test("复合操作 - select + where + union") {
    val persons1 = ZSetDB.fromIntIterable(List(
      Person("Alice", 25),
      Person("Bob", 30),
      Person("Charlie", 22)
    ))

    val persons2 = ZSetDB.fromIntIterable(List(
      Person("David", 28),
      Person("Eve", 35)
    ))

    val result = persons1
      .union(persons2)
      .where(_.age >= 25)
      .select(_.name)

    assertEquals(result.underlying.entryCount, 4)
    assert(result.underlying.contains("Alice"))
    assert(result.underlying.contains("Bob"))
    assert(result.underlying.contains("David"))
    assert(result.underlying.contains("Eve"))
    assert(!result.underlying.contains("Charlie")) // 年龄22 < 25
  }

  test("复合操作 - groupBy + select") {
    val orders = ZSetDB.fromIntIterable(List(
      Order(1, 100, 50.0),
      Order(2, 100, 75.0),
      Order(3, 101, 25.0),
      Order(4, 101, 100.0)
    ))

    val customerOrderCounts = orders
      .groupBy(_.customerId)
      .select { case (customerId, count) => s"Customer$customerId: $count orders" }

    assertEquals(customerOrderCounts.underlying.entryCount, 2)
    assert(customerOrderCounts.underlying.contains("Customer100: 2 orders"))
    assert(customerOrderCounts.underlying.contains("Customer101: 2 orders"))
  }

}
