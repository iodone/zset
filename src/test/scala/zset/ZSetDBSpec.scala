package com.example.zset

import munit.FunSuite
import com.example.zset.{WeightType, ZSet, ZSetDB}
import WeightType.IntegerWeight

class ZSetDBSpec extends FunSuite {

  // Test data models
  case class Person(name: String, age: Int)
  case class Department(name: String, budget: Int)
  case class Employee(name: String, departmentName: String, salary: Int)

  // Helper to create ZSet from simple data
  def createZSet[T](items: T*): ZSet[T, Int] =
    ZSet.fromPairs(items.map((_, 1)).toList)

  def createWeightedZSet[T](items: (T, Int)*): ZSet[T, Int] =
    ZSet.fromPairs(items.toList)

  test("select operation should map elements correctly") {
    val persons = createZSet(
      Person("Alice", 25),
      Person("Bob", 30),
      Person("Charlie", 35)
    )

    val names = ZSetDB.zSetDatabase.select(persons, (p: Person) => p.name)

    val expectedNames = createZSet("Alice", "Bob", "Charlie")
    assertEquals(names.entryCount, expectedNames.entryCount)
    assert(names.contains("Alice"))
    assert(names.contains("Bob"))
    assert(names.contains("Charlie"))
  }

  test("where/filter operation should filter elements correctly") {
    val persons = createZSet(
      Person("Alice", 25),
      Person("Bob", 30),
      Person("Charlie", 35)
    )

    val adults = ZSetDB.zSetDatabase.where(persons, (p: Person) => p.age >= 30)

    assertEquals(adults.entryCount, 2)
    assert(adults.contains(Person("Bob", 30)))
    assert(adults.contains(Person("Charlie", 35)))
    assert(!adults.contains(Person("Alice", 25)))
  }

  test("filter operation should be alias for where") {
    val numbers = createZSet(1, 2, 3, 4, 5)

    val evenWhere  = ZSetDB.zSetDatabase.where(numbers, (n: Int) => n % 2 == 0)
    val evenFilter = ZSetDB.zSetDatabase.filter(numbers, (n: Int) => n % 2 == 0)

    assertEquals(evenWhere.entryCount, evenFilter.entryCount)
    assertEquals(evenWhere.entries.toSet, evenFilter.entries.toSet)
  }

  test("cartesianProduct should generate all combinations with correct weights") {
    val letters = createWeightedZSet(("A", 2), ("B", 3))
    val numbers = createWeightedZSet((1, 4), (2, 5))

    val product = ZSetDB.zSetDatabase.cartesianProduct(
      letters,
      numbers,
      (letter: String, number: Int) => s"$letter$number"
    )

    assertEquals(product.entryCount, 4)
    assertEquals(product.getWeight("A1"), 8)  // 2 * 4
    assertEquals(product.getWeight("A2"), 10) // 2 * 5
    assertEquals(product.getWeight("B1"), 12) // 3 * 4
    assertEquals(product.getWeight("B2"), 15) // 3 * 5
  }

  test("multiply should be alias for cartesianProduct") {
    val left  = createZSet("x", "y")
    val right = createZSet(1, 2)

    val product  = ZSetDB.zSetDatabase.cartesianProduct(left, right, (s: String, i: Int) => (s, i))
    val multiply = ZSetDB.zSetDatabase.multiply(left, right, (s: String, i: Int) => (s, i))

    assertEquals(product.entryCount, multiply.entryCount)
    assertEquals(product.entries.toSet, multiply.entries.toSet)
  }

  test("union should combine ZSets and remove duplicates") {
    val set1 = createWeightedZSet(("A", 2), ("B", 3))
    val set2 = createWeightedZSet(("B", 4), ("C", 5))

    val result = ZSetDB.zSetDatabase.union(set1, set2)

    // Union should be distinct, so B should appear only once
    assertEquals(result.entryCount, 3)
    assert(result.contains("A"))
    assert(result.contains("B"))
    assert(result.contains("C"))
  }

  test("unionAll should combine ZSets keeping all entries") {
    val set1 = createWeightedZSet(("A", 2), ("B", 3))
    val set2 = createWeightedZSet(("B", 4), ("C", 5))

    val result = ZSetDB.zSetDatabase.unionAll(set1, set2)

    // unionAll should add weights for duplicates
    assertEquals(result.getWeight("A"), 2)
    assertEquals(result.getWeight("B"), 7) // 3 + 4
    assertEquals(result.getWeight("C"), 5)
  }

  test("intersect should keep only common elements") {
    val set1 = createZSet("A", "B", "C")
    val set2 = createZSet("B", "C", "D")

    val result = ZSetDB.zSetDatabase.intersect(set1, set2)

    assertEquals(result.entryCount, 2)
    assert(result.contains("B"))
    assert(result.contains("C"))
    assert(!result.contains("A"))
    assert(!result.contains("D"))
  }

  test("except should remove elements present in right set") {
    val set1 = createZSet("A", "B", "C")
    val set2 = createZSet("B", "D")

    val result = ZSetDB.zSetDatabase.except(set1, set2)

    assertEquals(result.entryCount, 2)
    assert(result.contains("A"))
    assert(result.contains("C"))
    assert(!result.contains("B"))
    assert(!result.contains("D"))
  }

  test("join should combine matching key-value pairs") {
    val employees = ZSet.fromPairs(
      List(
        (("Engineering", Employee("Alice", "Engineering", 80000)), 1),
        (("Marketing", Employee("Bob", "Marketing", 60000)), 1),
        (("Engineering", Employee("Charlie", "Engineering", 85000)), 1)
      )
    )

    val departments = ZSet.fromPairs(
      List(
        (("Engineering", Department("Engineering", 1000000)), 1),
        (("Marketing", Department("Marketing", 500000)), 1)
      )
    )

    val joined = ZSetDB.zSetDatabase.join(employees, departments)

    assertEquals(joined.entryCount, 3)

    // Check that all joins have correct structure
    joined.entries.foreach { case ((deptName, employee, department), weight) =>
      assertEquals(employee.departmentName, deptName)
      assertEquals(department.name, deptName)
      assertEquals(weight, 1)
    }
  }

  test("groupBy should group elements by key and count occurrences") {
    val persons = createZSet(
      Person("Alice", 25),
      Person("Bob", 30),
      Person("Charlie", 25),
      Person("David", 30)
    )

    val grouped = ZSetDB.zSetDatabase.groupBy(persons, (p: Person) => p.age)

    assertEquals(grouped.entryCount, 2)
    assert(grouped.contains((25, 2)))
    assert(grouped.contains((30, 2)))
  }

  test("count should create pairs of data with count 1") {
    val items = createWeightedZSet(("A", 3), ("B", 5), ("C", 1))

    val counted = ZSetDB.zSetDatabase.count(items)

    // Each distinct entry should be paired with count 1
    assertEquals(counted.entryCount, 3)
    assert(counted.contains(("A", 1)))
    assert(counted.contains(("B", 1)))
    assert(counted.contains(("C", 1)))
  }

  test("distinct should remove duplicate entries") {
    val items = createWeightedZSet(("A", 3), ("B", 5), ("A", 2))

    val distinct = ZSetDB.zSetDatabase.distinct(items)

    assertEquals(distinct.entryCount, 2)
    assert(distinct.contains("A"))
    assert(distinct.contains("B"))
    assertEquals(distinct.getWeight("A"), 1)
    assertEquals(distinct.getWeight("B"), 1)
  }

  test("sortBy should return sorted list of elements") {
    val persons = createZSet(
      Person("Charlie", 35),
      Person("Alice", 25),
      Person("Bob", 30)
    )

    val sortedByName = ZSetDB.zSetDatabase.sortBy(persons, (p: Person) => p.name)
    val sortedByAge  = ZSetDB.zSetDatabase.sortBy(persons, (p: Person) => p.age)

    assertEquals(sortedByName.length, 3)
    assertEquals(sortedByName(0).name, "Alice")
    assertEquals(sortedByName(1).name, "Bob")
    assertEquals(sortedByName(2).name, "Charlie")

    assertEquals(sortedByAge.length, 3)
    assertEquals(sortedByAge(0).age, 25)
    assertEquals(sortedByAge(1).age, 30)
    assertEquals(sortedByAge(2).age, 35)
  }

  test("take should return first n elements") {
    val numbers = createWeightedZSet((1, 10), (2, 20), (3, 30), (4, 40), (5, 50))

    val firstThree = ZSetDB.zSetDatabase.take(numbers, 3)

    assertEquals(firstThree.entryCount, 3)

    // Take should preserve the first n entries in order
    val entries = firstThree.entries.toList
    assertEquals(entries.length, 3)
  }

  test("take with n larger than collection size should return all elements") {
    val numbers = createZSet(1, 2, 3)

    val result = ZSetDB.zSetDatabase.take(numbers, 10)

    assertEquals(result.entryCount, 3)
    assert(result.contains(1))
    assert(result.contains(2))
    assert(result.contains(3))
  }

  test("take with n=0 should return empty ZSet") {
    val numbers = createZSet(1, 2, 3)

    val result = ZSetDB.zSetDatabase.take(numbers, 0)

    assertEquals(result.entryCount, 0)
    assert(result.isEmpty)
  }

  test("complex query combining multiple operations") {
    val employees = createZSet(
      Employee("Alice", "Engineering", 80000),
      Employee("Bob", "Marketing", 60000),
      Employee("Charlie", "Engineering", 85000),
      Employee("David", "Sales", 55000),
      Employee("Eve", "Engineering", 90000)
    )

    // Find high-paid engineers (salary >= 80000) and get their names
    val highPaidEngineers = ZSetDB.zSetDatabase
      .where(employees, (e: Employee) => e.departmentName == "Engineering")

    val highPaidOnly = ZSetDB.zSetDatabase
      .where(highPaidEngineers, (e: Employee) => e.salary >= 80000)

    val names = ZSetDB.zSetDatabase
      .select(highPaidOnly, (e: Employee) => e.name)

    assertEquals(names.entryCount, 3)
    assert(names.contains("Alice"))
    assert(names.contains("Charlie"))
    assert(names.contains("Eve"))
    assert(!names.contains("Bob"))
    assert(!names.contains("David"))
  }

  // ============== Aggregate API Tests ==============
  // 基于 Feldera 博客的聚合操作测试

  test("basic aggregate API should work correctly") {
    val persons = createZSet(
      Person("Billy", 28),
      Person("Barbara", 36),
      Person("John", 12)
    )

    // COUNT 聚合 - 对应 Feldera 的 COUNT aggregation
    val count = ZSetDB.zSetDatabase.aggregate(persons, 0, (acc: Int, p: Person) => acc + 1)
    assertEquals(count, 3)

    // SUM 聚合 - 对应 Feldera 的 SUM aggregation
    val ageSum = ZSetDB.zSetDatabase.aggregate(persons, 0, (acc: Int, p: Person) => acc + p.age)
    assertEquals(ageSum, 76) // 28 + 36 + 12

    // Custom aggregation - find oldest person
    val maxAge = ZSetDB.zSetDatabase.aggregate(persons, 0, (acc: Int, p: Person) => math.max(acc, p.age))
    assertEquals(maxAge, 36)
  }

  test("sum aggregation should calculate weighted sum correctly") {
    case class Item(name: String, price: Double, quantity: Int)
    val items = createZSet(
      Item("Apple", 1.5, 10),
      Item("Banana", 0.8, 15),
      Item("Orange", 2.0, 8)
    )

    val totalPrice = ZSetDB.zSetDatabase.sum(items, (item: Item) => item.price)
    assertEquals(totalPrice, 4.3, 0.001) // 1.5 + 0.8 + 2.0

    val totalQuantity = ZSetDB.zSetDatabase.sum(items, (item: Item) => item.quantity)
    assertEquals(totalQuantity, 33) // 10 + 15 + 8
  }

  test("max aggregation should find maximum value") {
    val persons = createZSet(
      Person("Alice", 25),
      Person("Bob", 30),
      Person("Charlie", 20),
      Person("Diana", 35)
    )

    val maxAge = ZSetDB.zSetDatabase.max(persons, (p: Person) => p.age)
    assertEquals(maxAge, Some(35))

    val maxName = ZSetDB.zSetDatabase.max(persons, (p: Person) => p.name)
    assertEquals(maxName, Some("Diana")) // 字典序最大
  }

  test("min aggregation should find minimum value") {
    val persons = createZSet(
      Person("Alice", 25),
      Person("Bob", 30),
      Person("Charlie", 20),
      Person("Diana", 35)
    )

    val minAge = ZSetDB.zSetDatabase.min(persons, (p: Person) => p.age)
    assertEquals(minAge, Some(20))

    val minName = ZSetDB.zSetDatabase.min(persons, (p: Person) => p.name)
    assertEquals(minName, Some("Alice")) // 字典序最小
  }

  test("avg aggregation should calculate average correctly") {
    val persons = createZSet(
      Person("Alice", 20),
      Person("Bob", 30),
      Person("Charlie", 40)
    )

    val avgAge = ZSetDB.zSetDatabase.avg(persons, (p: Person) => p.age)
    assertEquals(avgAge, Some(30.0)) // (20 + 30 + 40) / 3 = 30

    // Test with empty ZSet
    val empty = ZSet.empty[Person, Int]
    val emptyAvg = ZSetDB.zSetDatabase.avg(empty, (p: Person) => p.age)
    assertEquals(emptyAvg, None)
  }

  test("weight-aware aggregations should handle ZSet weights correctly") {
    // 创建带权重的 ZSet
    case class Sale(product: String, amount: Double)
    val sales = createWeightedZSet(
      (Sale("Product A", 100.0), 3), // 权重为 3，表示有 3 笔这样的销售
      (Sale("Product B", 200.0), 2), // 权重为 2，表示有 2 笔这样的销售
      (Sale("Product C", 150.0), 1)  // 权重为 1，表示有 1 笔这样的销售
    )

    // 注意：当前的 aggregate API 不考虑权重，只处理唯一元素
    // 所以结果应该是基于 3 个不同的 Sale 对象
    val totalSales = ZSetDB.zSetDatabase.aggregate(sales, 0, (acc: Int, sale: Sale) => acc + 1)
    assertEquals(totalSales, 3) // 3 个不同的销售记录

    val totalAmount = ZSetDB.zSetDatabase.sum(sales, (sale: Sale) => sale.amount)
    assertEquals(totalAmount, 850.0) // 权重感知的总和：100*3 + 200*2 + 150*1 = 850

    val maxAmount = ZSetDB.zSetDatabase.max(sales, (sale: Sale) => sale.amount)
    assertEquals(maxAmount, Some(200.0))
  }

  test("aggregations on empty ZSet should handle edge cases") {
    val empty = ZSet.empty[Person, Int]

    val count = ZSetDB.zSetDatabase.aggregate(empty, 0, (acc: Int, p: Person) => acc + 1)
    assertEquals(count, 0)

    val sum = ZSetDB.zSetDatabase.sum(empty, (p: Person) => p.age)
    assertEquals(sum, 0)

    val max = ZSetDB.zSetDatabase.max(empty, (p: Person) => p.age)
    assertEquals(max, None)

    val min = ZSetDB.zSetDatabase.min(empty, (p: Person) => p.age)
    assertEquals(min, None)

    val avg = ZSetDB.zSetDatabase.avg(empty, (p: Person) => p.age)
    assertEquals(avg, None)
  }

  test("complex aggregation workflow - Feldera style") {
    // 模拟一个更复杂的聚合场景，类似 Feldera 博客中的例子
    case class Transaction(customerId: Int, amount: Double, category: String)
    val transactions = createZSet(
      Transaction(1, 100.0, "Electronics"),
      Transaction(1, 50.0, "Books"),
      Transaction(2, 200.0, "Electronics"),
      Transaction(2, 75.0, "Clothing"),
      Transaction(3, 300.0, "Electronics"),
      Transaction(3, 25.0, "Books")
    )

    // 1. 总交易金额
    val totalAmount = ZSetDB.zSetDatabase.sum(transactions, (t: Transaction) => t.amount)
    assertEquals(totalAmount, 750.0) // 100+50+200+75+300+25

    // 2. 最大交易金额
    val maxTransaction = ZSetDB.zSetDatabase.max(transactions, (t: Transaction) => t.amount)
    assertEquals(maxTransaction, Some(300.0))

    // 3. 平均交易金额
    val avgTransaction = ZSetDB.zSetDatabase.avg(transactions, (t: Transaction) => t.amount)
    assertEquals(avgTransaction, Some(125.0)) // 750/6

    // 4. 按类别分组
    val groupedByCategory = ZSetDB.zSetDatabase.groupBy(transactions, (t: Transaction) => t.category)
    assertEquals(groupedByCategory.entryCount, 3)
    assert(groupedByCategory.contains(("Electronics", 3)))
    assert(groupedByCategory.contains(("Books", 2)))
    assert(groupedByCategory.contains(("Clothing", 1)))

    // 5. 按客户分组
    val groupedByCustomer = ZSetDB.zSetDatabase.groupBy(transactions, (t: Transaction) => t.customerId)
    assertEquals(groupedByCustomer.entryCount, 3)
    assert(groupedByCustomer.contains((1, 2)))
    assert(groupedByCustomer.contains((2, 2)))
    assert(groupedByCustomer.contains((3, 2)))
  }
}
