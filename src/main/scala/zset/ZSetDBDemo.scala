package com.example.zset

import com.example.zset.ZSetDB.given
import com.example.zset.ZSetDBOps.*

object ZSetDBDemo extends App {

  // Data models
  case class Person(name: String, age: Int)
  case class Department(name: String, budget: Int)
  case class Employee(name: String, departmentName: String, salary: Int)

  println("=== ZSetDB Demo ===")

  // 创建包含人员信息的 ZSetDB
  val personDB = ZSetDB(
    Person("Alice", 25),
    Person("Bob", 30),
    Person("Charlie", 35),
    Person("David", 28)
  )

  println(s"原始人员数据: ${personDB.sortBy(_.name)}")

  // 使用 Database API 进行查询操作
  val names = personDB.select(_.name)
  println(s"人员姓名: ${names.sortBy(identity)}")

  val adults = personDB.where(_.age >= 30)
  println(s"30岁及以上的人员: ${adults.sortBy(_.name)}")

  // 聚合操作
  val totalAge = personDB.agg(0, (acc: Int, p: Person) => acc + p.age)
  println(s"年龄总和: $totalAge")

  val avgAge = personDB.avg(_.age.toDouble)
  println(s"平均年龄: $avgAge")

  val maxAge = personDB.max(_.age)
  println(s"最大年龄: $maxAge")

  val minAge = personDB.min(_.age)
  println(s"最小年龄: $minAge")

  // 创建部门数据
  val departmentDB = ZSetDB(
    Department("Engineering", 1000000),
    Department("Sales", 500000),
    Department("HR", 200000)
  )

  // 创建员工数据（包含部门信息）
  val employeeDB = ZSetDB(
    Employee("Alice", "Engineering", 80000),
    Employee("Bob", "Sales", 60000),
    Employee("Charlie", "Engineering", 90000),
    Employee("David", "HR", 50000)
  )

  println(s"\n部门数据: ${departmentDB.sortBy(_.name)}")
  println(s"员工数据: ${employeeDB.sortBy(_.name)}")

  // 部门预算总和
  val totalBudget = departmentDB.sum(_.budget)
  println(s"总预算: $totalBudget")

  // 工程部员工
  val engineeringEmployees = employeeDB.where(_.departmentName == "Engineering")
  println(s"工程部员工: ${engineeringEmployees.sortBy(_.name)}")

  // 按部门分组统计员工数量
  val employeesByDept = employeeDB.groupBy(_.departmentName)
  println(s"各部门员工数量: ${employeesByDept.sortBy(_._1)}")

  // Union操作示例
  val newPersons = ZSetDB(
    Person("Eve", 26),
    Person("Frank", 32)
  )
  val allPersons = personDB.union(newPersons)
  println(s"合并后的人员: ${allPersons.sortBy(_.name)}")

  // Distinct操作
  val duplicatePersons = ZSetDB(
    Person("Alice", 25),
    Person("Bob", 30),
    Person("Alice", 25) // 重复
  )
  val uniquePersons = duplicatePersons.distinct
  println(s"去重后的人员: ${uniquePersons.sortBy(_.name)}")

  // Cartesian Product示例
  val titles      = ZSetDB("Mr.", "Ms.")
  val shortNames  = personDB.select(_.name).take(2)
  val formalNames = titles.cartesianProduct(shortNames, (title, name) => s"$title $name")
  println(s"正式称呼: ${formalNames.sortBy(identity)}")

  // === 链式操作示例 ===
  println("\n=== 链式操作示例 ===")

  // 案例1: 数据分析管道 - 找出工程部高薪员工的平均工资
  val engineeringHighSalaryAvg = employeeDB
    .where(_.departmentName == "Engineering") // 过滤工程部
    .where(_.salary > 70000)                  // 过滤高薪员工
    .avg(_.salary.toDouble)                   // 计算平均工资
  println(s"工程部高薪员工平均工资: $engineeringHighSalaryAvg")

  // 案例2: 复杂过滤和投影链
  val seniorEmployeeNames = employeeDB
    .where(_.salary > 60000) // 高薪员工
    .select(_.name)          // 选择姓名
    .where(_.length > 5)     // 名字长度大于5
    .distinct                // 去重
  println(s"高薪且名字较长的员工: ${seniorEmployeeNames.sortBy(identity)}")

  // 案例3: 数据聚合和统计链
  case class SalaryStats(department: String, totalSalary: Int, employeeCount: Int, avgSalary: Double)

  val salaryStatsByDept = employeeDB
    .groupBy(_.departmentName) // 按部门分组
    .select { case (dept, count) =>
      val deptEmployees = employeeDB.where(_.departmentName == dept)
      val totalSalary   = deptEmployees.sum(_.salary)
      val avgSalary     = deptEmployees.avg(_.salary.toDouble).getOrElse(0.0)
      SalaryStats(dept, totalSalary, count, avgSalary)
    }
  println(s"各部门薪资统计: ${salaryStatsByDept.sortBy(_.avgSalary)}")

  // 案例4: 多表关联和数据融合
  case class DepartmentEmployee(empName: String, deptName: String, salary: Int, budget: Int)

  val departmentEmployeeInfo = employeeDB
    .select(emp => (emp.departmentName, emp))             // 准备join的key
    .join(departmentDB.select(dept => (dept.name, dept))) // 与部门表关联
    .select { case (deptName, emp, dept) =>
      DepartmentEmployee(emp.name, deptName, emp.salary, dept.budget)
    }
    .where(_.salary > 50000) // 过滤高薪员工
    .sortBy(_.salary)        // 按薪资排序
  println(s"高薪员工部门信息: ${departmentEmployeeInfo}")

  // 案例5: 复杂业务逻辑链 - 计算各部门薪资占预算比例
  case class BudgetRatio(department: String, totalSalary: Int, budget: Int, ratio: Double)

  val budgetRatios = departmentDB
    .select(_.name) // 获取部门名称
    .select { deptName =>
      val deptEmployees = employeeDB.where(_.departmentName == deptName)
      val totalSalary   = deptEmployees.sum(_.salary)
      val budget        = departmentDB.where(_.name == deptName).sum(_.budget)
      val ratio         = if (budget > 0) totalSalary.toDouble / budget else 0.0
      BudgetRatio(deptName, totalSalary, budget, ratio)
    }
    .where(_.ratio > 0) // 过滤有效比例
    .sortBy(_.ratio)    // 按比例排序
  println(s"各部门薪资预算比例: ${budgetRatios}")

  // 案例6: 数据验证和清洗链
  case class ValidEmployee(name: String, department: String, salary: Int, salaryLevel: String)

  val validatedEmployees = employeeDB
    .where(_.name.nonEmpty)           // 验证姓名非空
    .where(_.salary > 0)              // 验证薪资有效
    .where(_.departmentName.nonEmpty) // 验证部门非空
    .select { emp =>
      val level = emp.salary match {
        case s if s >= 80000 => "Senior"
        case s if s >= 60000 => "Mid"
        case _               => "Junior"
      }
      ValidEmployee(emp.name, emp.departmentName, emp.salary, level)
    }
    .groupBy(_.salaryLevel) // 按级别分组
    .select { case (level, count) => s"$level: $count 人" }
    .sortBy(identity)
  println(s"员工级别分布: ${validatedEmployees}")

  // 案例7: 异常检测链 - 找出薪资异常的员工
  val avgSalaryOverall = employeeDB.avg(_.salary.toDouble).getOrElse(0.0)
  val salaryStdDev = {
    val salariesDB = employeeDB.select(_.salary.toDouble)
    val variance = salariesDB.agg(
      0.0,
      (acc, salary) => acc + math.pow(salary - avgSalaryOverall, 2)
    ) / salariesDB.underlying.entryCount
    math.sqrt(variance)
  }

  val outlierEmployees = employeeDB
    .where(emp => math.abs(emp.salary - avgSalaryOverall) > salaryStdDev * 1.5) // 1.5倍标准差
    .select(emp => s"${emp.name} (${emp.departmentName}): ${emp.salary}")
    .sortBy(identity)
  println(s"薪资异常员工 (偏离均值>1.5σ): $outlierEmployees")

  // 案例8: 数据聚合和排名链
  case class DepartmentRanking(department: String, avgSalary: Double, employeeCount: Int, rank: Int)

  val departmentRankings = departmentDB
    .select(_.name) // 获取部门名称
    .select { deptName =>
      val deptEmployees = employeeDB.where(_.departmentName == deptName)
      val avgSalary     = deptEmployees.avg(_.salary.toDouble).getOrElse(0.0)
      val count         = deptEmployees.underlying.entryCount
      (deptName, avgSalary, count)
    }
    .take(10)     // 取前10个
    .sortBy(_._2) // 按平均薪资排序

  val rankedDepartmentsList = departmentRankings.zipWithIndex.map {
    case ((dept, avgSal, count), index) =>
      DepartmentRanking(dept, avgSal, count, index + 1)
  }
  println(s"部门薪资排名: $rankedDepartmentsList")

  // 案例9: 交集和差集操作链
  val highSalaryEmps  = employeeDB.where(_.salary > 70000).select(_.name)
  val engineeringEmps = employeeDB.where(_.departmentName == "Engineering").select(_.name)

  val highSalaryEngineers = highSalaryEmps
    .intersect(engineeringEmps) // 高薪工程师
    .sortBy(identity)
  println(s"高薪工程师: $highSalaryEngineers")

  val nonEngineeringHighSalary = highSalaryEmps
    .except(engineeringEmps) // 非工程部的高薪员工
    .sortBy(identity)
  println(s"非工程部高薪员工: $nonEngineeringHighSalary")

  // 案例10: 数据转换和格式化链
  case class EmployeeReport(info: String)

  val employeeReportsStrings = employeeDB
    .where(_.salary > 60000) // 过滤条件
    .select { emp =>
      val salaryLevel = if (emp.salary > 80000) "★★★" else if (emp.salary > 70000) "★★" else "★"
      s"${emp.name} | ${emp.departmentName} | $${emp.salary} $salaryLevel"
    }
    .distinct // 去重

  val employeeReports = ZSetDB.fromIntIterable(
    employeeReportsStrings.sortBy(identity).map(EmployeeReport(_))
  )
  println(s"员工报告:")
  employeeReports.sortBy(_.info).foreach(report => println(s"  ${report.info}"))

  println("\n=== 链式操作演示完成 ===")
}
