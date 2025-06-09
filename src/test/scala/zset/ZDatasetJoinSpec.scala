package com.example.zset

import munit.FunSuite
import com.example.zset.{WeightType, ZDataset, ZSet}
import WeightType.{IntegerWeight, LongWeight, given}
import com.example.zset.ZDatasetOps.*

class ZDatasetJoinSpec extends FunSuite {

  // 测试数据类型定义
  case class Customer(id: Int, name: String, city: String)
  case class Order(id: Int, customerId: Int, amount: Double, product: String)
  case class Product(id: Int, name: String, price: Double, category: String)
  case class Employee(id: Int, name: String, departmentId: Int, salary: Int)
  case class Department(id: Int, name: String, budget: Int)

  // === 基础JOIN测试 ===
  test("join - 基本等值连接") {
    val customers = ZDataset.fromIntIterable(
      List(
        Customer(1, "Alice", "NY"),
        Customer(2, "Bob", "LA"),
        Customer(3, "Charlie", "SF")
      )
    )

    val orders = ZDataset.fromIntIterable(
      List(
        Order(101, 1, 100.0, "Laptop"),
        Order(102, 2, 50.0, "Mouse"),
        Order(103, 1, 200.0, "Monitor")
      )
    )

    val result = customers.join(
      orders,
      customers.on(_.id) == orders.on(_.customerId),
      (customer: Customer, order: Order) => (customer.name, order.product, order.amount)
    )

    assertEquals(result.underlying.entryCount, 3)
    assert(result.underlying.contains(("Alice", "Laptop", 100.0)))
    assert(result.underlying.contains(("Bob", "Mouse", 50.0)))
    assert(result.underlying.contains(("Alice", "Monitor", 200.0)))
  }

  test("join - 员工部门连接") {
    val employees = ZDataset.fromIntIterable(
      List(
        Employee(1, "John", 10, 50000),
        Employee(2, "Jane", 20, 60000),
        Employee(3, "Mike", 10, 55000)
      )
    )

    val departments = ZDataset.fromIntIterable(
      List(
        Department(10, "Engineering", 1000000),
        Department(20, "Sales", 500000),
        Department(30, "HR", 200000)
      )
    )

    val result = employees.join(
      departments,
      employees.on(_.departmentId) == departments.on(_.id),
      (emp: Employee, dept: Department) => (emp.name, dept.name, emp.salary)
    )

    assertEquals(result.underlying.entryCount, 3)
    assert(result.underlying.contains(("John", "Engineering", 50000)))
    assert(result.underlying.contains(("Jane", "Sales", 60000)))
    assert(result.underlying.contains(("Mike", "Engineering", 55000)))
  }

  test("join - 多对一连接") {
    val orders = ZDataset.fromIntIterable(
      List(
        Order(1, 1, 100.0, "Laptop"),
        Order(2, 1, 50.0, "Mouse"),
        Order(3, 2, 200.0, "Monitor"),
        Order(4, 2, 30.0, "Cable")
      )
    )

    val customers = ZDataset.fromIntIterable(
      List(
        Customer(1, "Alice", "NY"),
        Customer(2, "Bob", "LA")
      )
    )

    val result = orders.join(
      customers,
      orders.on(_.customerId) == customers.on(_.id),
      (order: Order, customer: Customer) => (customer.name, order.product, order.amount)
    )

    assertEquals(result.underlying.entryCount, 4)
    assert(result.underlying.contains(("Alice", "Laptop", 100.0)))
    assert(result.underlying.contains(("Alice", "Mouse", 50.0)))
    assert(result.underlying.contains(("Bob", "Monitor", 200.0)))
    assert(result.underlying.contains(("Bob", "Cable", 30.0)))
  }

  test("join - 无匹配记录") {
    val customers = ZDataset.fromIntIterable(
      List(
        Customer(1, "Alice", "NY"),
        Customer(2, "Bob", "LA")
      )
    )

    val orders = ZDataset.fromIntIterable(
      List(
        Order(101, 3, 100.0, "Laptop"), // customerId=3 不存在
        Order(102, 4, 50.0, "Mouse")    // customerId=4 不存在
      )
    )

    val result = customers.join(
      orders,
      customers.on(_.id) == orders.on(_.customerId),
      (customer: Customer, order: Order) => (customer.name, order.product)
    )

    assertEquals(result.underlying.entryCount, 0)
    assert(result.underlying.isEmpty)
  }

  // === JOIN + GROUPBY 组合测试 ===
  test("join + groupBy - 客户订单统计") {
    val customers = ZDataset.fromIntIterable(
      List(
        Customer(1, "Alice", "NY"),
        Customer(2, "Bob", "LA"),
        Customer(3, "Charlie", "SF")
      )
    )

    val orders = ZDataset.fromIntIterable(
      List(
        Order(101, 1, 100.0, "Laptop"),
        Order(102, 1, 50.0, "Mouse"),
        Order(103, 2, 200.0, "Monitor"),
        Order(104, 1, 30.0, "Cable")
      )
    )

    // 首先进行join
    val customerOrders = customers.join(
      orders,
      customers.on(_.id) == orders.on(_.customerId),
      (customer: Customer, order: Order) => (customer.name, order.amount)
    )

    // 然后按客户分组并统计总金额
    val customerStats = customerOrders.groupBy(_._1).sum(_._2)

    assertEquals(customerStats.keyCount, 2)
    assertEquals(customerStats.getZSet("Alice").sum(identity), 180.0) // 100 + 50 + 30
    assertEquals(customerStats.getZSet("Bob").sum(identity), 200.0)   // 200
  }

  test("join + groupBy + agg - 部门薪资统计") {
    val employees = ZDataset.fromIntIterable(
      List(
        Employee(1, "John", 10, 50000),
        Employee(2, "Jane", 20, 60000),
        Employee(3, "Mike", 10, 55000),
        Employee(4, "Sarah", 20, 65000),
        Employee(5, "Tom", 30, 45000)
      )
    )

    val departments = ZDataset.fromIntIterable(
      List(
        Department(10, "Engineering", 1000000),
        Department(20, "Sales", 500000),
        Department(30, "HR", 200000)
      )
    )

    // Join员工和部门
    val empDept = employees.join(
      departments,
      employees.on(_.departmentId) == departments.on(_.id),
      (emp: Employee, dept: Department) => (dept.name, emp.salary, dept.budget)
    )

    // 按部门分组统计
    val deptStats = empDept.groupBy(_._1)

    // 计算各部门总薪资
    val totalSalaries = deptStats.sum(_._2)
    assertEquals(totalSalaries.getZSet("Engineering").sum(identity), 105000) // 50000 + 55000
    assertEquals(totalSalaries.getZSet("Sales").sum(identity), 125000)       // 60000 + 65000
    assertEquals(totalSalaries.getZSet("HR").sum(identity), 45000)           // 45000

    // 计算各部门平均薪资
    val avgSalaries = deptStats.avg(_._2)
    assert(avgSalaries.getZSet("Engineering").underlying.contains(Some(52500.0)))
    assert(avgSalaries.getZSet("Sales").underlying.contains(Some(62500.0)))
    assert(avgSalaries.getZSet("HR").underlying.contains(Some(45000.0)))
  }

  // === 多表JOIN测试 ===
  test("三表join - 客户订单产品") {
    val customers = ZDataset.fromIntIterable(
      List(
        Customer(1, "Alice", "NY"),
        Customer(2, "Bob", "LA")
      )
    )

    val orders = ZDataset.fromIntIterable(
      List(
        Order(101, 1, 100.0, "Laptop"),
        Order(102, 2, 50.0, "Mouse")
      )
    )

    val products = ZDataset.fromIntIterable(
      List(
        Product(1, "Laptop", 999.0, "Electronics"),
        Product(2, "Mouse", 29.0, "Electronics")
      )
    )

    // 先join客户和订单
    val customerOrders = customers.join(
      orders,
      customers.on(_.id) == orders.on(_.customerId),
      (customer: Customer, order: Order) => (customer.name, order.product, order.amount)
    )

    // 再join产品信息（通过产品名称）
    val fullInfo = customerOrders.join(
      products,
      customerOrders.on(_._2) == products.on(_.name),
      (co: (String, String, Double), product: Product) =>
        (co._1, co._2, co._3, product.price, product.category)
    )

    assertEquals(fullInfo.underlying.entryCount, 2)
    assert(fullInfo.underlying.contains(("Alice", "Laptop", 100.0, 999.0, "Electronics")))
    assert(fullInfo.underlying.contains(("Bob", "Mouse", 50.0, 29.0, "Electronics")))
  }

  // === JOIN + WHERE 组合测试 ===
  test("join + where - 高薪员工部门信息") {
    val employees = ZDataset.fromIntIterable(
      List(
        Employee(1, "John", 10, 80000),
        Employee(2, "Jane", 20, 45000),
        Employee(3, "Mike", 10, 90000),
        Employee(4, "Sarah", 20, 70000)
      )
    )

    val departments = ZDataset.fromIntIterable(
      List(
        Department(10, "Engineering", 1000000),
        Department(20, "Sales", 500000)
      )
    )

    val highSalaryEmpDept = employees
      .where(_.salary >= 70000) // 先过滤高薪员工
      .join(
        departments,
        employees.on(_.departmentId) == departments.on(_.id),
        (emp: Employee, dept: Department) => (emp.name, dept.name, emp.salary)
      )

    assertEquals(highSalaryEmpDept.underlying.entryCount, 3)
    assert(highSalaryEmpDept.underlying.contains(("John", "Engineering", 80000)))
    assert(highSalaryEmpDept.underlying.contains(("Mike", "Engineering", 90000)))
    assert(highSalaryEmpDept.underlying.contains(("Sarah", "Sales", 70000)))
  }

  // === JOIN + SELECT 组合测试 ===
  test("join + select - 数据投影") {
    val employees = ZDataset.fromIntIterable(
      List(
        Employee(1, "John", 10, 50000),
        Employee(2, "Jane", 20, 60000)
      )
    )

    val departments = ZDataset.fromIntIterable(
      List(
        Department(10, "Engineering", 1000000),
        Department(20, "Sales", 500000)
      )
    )

    val empDeptNames = employees
      .join(
        departments,
        employees.on(_.departmentId) == departments.on(_.id),
        (emp: Employee, dept: Department) => (emp.name, dept.name, emp.salary)
      )
      .select { case (empName, deptName, salary) => s"$empName works in $deptName" }

    assertEquals(empDeptNames.underlying.entryCount, 2)
    assert(empDeptNames.underlying.contains("John works in Engineering"))
    assert(empDeptNames.underlying.contains("Jane works in Sales"))
  }

  // === 复杂业务场景测试 ===
  test("复杂场景 - 部门预算使用率分析") {
    val employees = ZDataset.fromIntIterable(
      List(
        Employee(1, "John", 10, 50000),
        Employee(2, "Jane", 20, 60000),
        Employee(3, "Mike", 10, 55000),
        Employee(4, "Sarah", 20, 65000)
      )
    )

    val departments = ZDataset.fromIntIterable(
      List(
        Department(10, "Engineering", 200000),
        Department(20, "Sales", 100000)
      )
    )

    // 计算各部门预算使用率
    val budgetUsage = employees
      .join(
        departments,
        employees.on(_.departmentId) == departments.on(_.id),
        (emp: Employee, dept: Department) => (dept.name, emp.salary, dept.budget)
      )
      .groupBy(_._1) // 按部门分组
      .aggregate((0, 0)) { case ((totalSalary, budget), (_, salary, deptBudget), _) =>
        (totalSalary + salary, deptBudget)
      }

    // 验证预算使用率计算
    val engUsage   = budgetUsage.getZSet("Engineering").underlying.entries.head._1
    val salesUsage = budgetUsage.getZSet("Sales").underlying.entries.head._1

    // 计算使用率百分比
    val engUsagePercent   = (engUsage._1.toDouble / engUsage._2 * 100).toInt
    val salesUsagePercent = (salesUsage._1.toDouble / salesUsage._2 * 100).toInt

    assertEquals(engUsagePercent, 52)    // (50000 + 55000) / 200000 * 100 = 52.5% ≈ 52%
    assertEquals(salesUsagePercent, 125) // (60000 + 65000) / 100000 * 100 = 125%
  }

  test("链式操作 - join + groupBy + where + select") {
    val customers = ZDataset.fromIntIterable(
      List(
        Customer(1, "Alice", "NY"),
        Customer(2, "Bob", "LA"),
        Customer(3, "Charlie", "NY")
      )
    )

    val orders = ZDataset.fromIntIterable(
      List(
        Order(101, 1, 100.0, "Laptop"),
        Order(102, 1, 50.0, "Mouse"),
        Order(103, 2, 200.0, "Monitor"),
        Order(104, 3, 300.0, "Keyboard"),
        Order(105, 3, 80.0, "Cable")
      )
    )

    // 复杂链式操作：按城市统计高消费客户
    val cityHighSpenders = customers
      .join(
        orders,
        customers.on(_.id) == orders.on(_.customerId),
        (customer: Customer, order: Order) => (customer.city, customer.name, order.amount)
      )
      .groupBy(_._1)          // 按城市分组
      .sum(_._3)              // 计算每个城市的总消费
      .filterByKey(_ => true) // 保留所有城市
      .flatten { case (city, totalAmount) => (city, totalAmount) }
      .where(_._2 >= 200.0) // 过滤总消费>=200的城市
      .select { case (city, amount) => s"$city: $$$amount" }

    assertEquals(cityHighSpenders.underlying.entryCount, 2)
    assert(cityHighSpenders.underlying.contains("NY: $530.0")) // Alice: 150 + Charlie: 380 = 530
    assert(cityHighSpenders.underlying.contains("LA: $200.0")) // Bob: 200
  }

  // === 性能和边界情况测试 ===
  test("join - 空数据集") {
    val emptyCustomers = ZDataset.empty[Customer, Int]
    val orders = ZDataset.fromIntIterable(
      List(
        Order(101, 1, 100.0, "Laptop")
      )
    )

    val result = emptyCustomers.join(
      orders,
      emptyCustomers.on(_.id) == orders.on(_.customerId),
      (customer: Customer, order: Order) => (customer.name, order.product)
    )

    assertEquals(result.underlying.entryCount, 0)
    assert(result.underlying.isEmpty)
  }

  test("join - 重复数据处理") {
    val customers = ZDataset.fromPairs(
      List(
        (Customer(1, "Alice", "NY"), 2), // 重复客户
        (Customer(2, "Bob", "LA"), 1)
      )
    )

    val orders = ZDataset.fromIntIterable(
      List(
        Order(101, 1, 100.0, "Laptop")
      )
    )

    val result = customers.join(
      orders,
      customers.on(_.id) == orders.on(_.customerId),
      (customer: Customer, order: Order) => (customer.name, order.product)
    )

    // 由于customers中有重复的Alice记录，join结果也会有对应的权重
    assertEquals(result.underlying.entryCount, 1)
    assertEquals(result.underlying.getWeight(("Alice", "Laptop")), 2)
  }

  // === 字符串字段JOIN测试 ===
  test("join - 字符串字段连接") {
    val products = ZDataset.fromIntIterable(
      List(
        Product(1, "Laptop", 999.0, "Electronics"),
        Product(2, "Mouse", 29.0, "Electronics"),
        Product(3, "Desk", 199.0, "Furniture")
      )
    )

    val orders = ZDataset.fromIntIterable(
      List(
        Order(101, 1, 100.0, "Laptop"),
        Order(102, 2, 50.0, "Mouse"),
        Order(103, 3, 80.0, "Unknown") // 产品不存在
      )
    )

    val result = orders.join(
      products,
      orders.on(_.product) == products.on(_.name),
      (order: Order, product: Product) =>
        (order.customerId, product.name, product.category, order.amount)
    )

    assertEquals(result.underlying.entryCount, 2)
    assert(result.underlying.contains((1, "Laptop", "Electronics", 100.0)))
    assert(result.underlying.contains((2, "Mouse", "Electronics", 50.0)))
    // "Unknown"产品没有匹配，所以不在结果中
  }

  // === 数值范围JOIN测试（模拟） ===
  test("join - 复杂条件模拟") {
    // 员工薪资等级
    case class SalaryGrade(minSalary: Int, maxSalary: Int, grade: String)

    val employees = ZDataset.fromIntIterable(
      List(
        Employee(1, "John", 10, 45000),
        Employee(2, "Jane", 20, 65000),
        Employee(3, "Mike", 10, 85000)
      )
    )

    val grades = ZDataset.fromIntIterable(
      List(
        SalaryGrade(40000, 59999, "Junior"),
        SalaryGrade(60000, 79999, "Senior"),
        SalaryGrade(80000, 999999, "Principal")
      )
    )

    // 由于我们的join只支持等值连接，这里用一个技巧来模拟范围连接
    // 先将员工薪资分档，然后join
    val empWithGrade = employees.select { emp =>
      val gradeKey =
        if (emp.salary < 60000) "Junior"
        else if (emp.salary < 80000) "Senior"
        else "Principal"
      (emp.name, emp.salary, gradeKey)
    }

    val gradeMap = grades.select(g => (g.grade, g))

    val result = empWithGrade.join(
      gradeMap,
      empWithGrade.on(_._3) == gradeMap.on(_._1),
      (emp: (String, Int, String), grade: (String, SalaryGrade)) => (emp._1, emp._2, grade._2.grade)
    )

    assertEquals(result.underlying.entryCount, 3)
    assert(result.underlying.contains(("John", 45000, "Junior")))
    assert(result.underlying.contains(("Jane", 65000, "Senior")))
    assert(result.underlying.contains(("Mike", 85000, "Principal")))
  }
}
