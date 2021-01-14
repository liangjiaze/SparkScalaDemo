package cn.jade.wordCount

/**
  *作业题
  */
object ListTest {
  def main(args: Array[String]): Unit = {
    //创建一个List
    val list0=List(1,7,9,8,0,3,5,4,6,2)

    //将list0中的每一个元素乘以10后生成一个新的集合
    val list1=list0.map(x=>x*10)
    println("list1==== "+list1)

    //将list0中的偶数取出来生成一个新的集合
    val list2=list0.filter(x=>x%2==0)
    println("list2==== "+list2)

    //将list0排序后生成一个新的集合
    val list3=list0.sorted
    val list4=list0.sortBy(x=>x)
    val list5=list0.sortWith((x,y)=>x<y)
    println("list3==== "+list3)
    println("list4==== "+list4)
    println("list5==== "+list5)

    //反转顺序
    val list6=list3.reverse
    println("list6==== "+list6)

    //将list0中的元素4个一组,类型为Iterator[List[Int]]
    val list7=list0.grouped(4)
    println("list7==== "+list7)

    //将Iterator转换成List
    val list8=list7.toList
    println("list8==== "+list8)

    //将多个list压扁成一个List
    val list9=list8.flatten
    println("list9==== "+list9)

    val lines = List("hello tom hello jerry", "hello jerry", "hello kitty")
    //先按空格切分，在压平
    val result1=lines.flatMap(_.split(" "))
    println("result1==== "+result1)

    //并行计算求和
    val result2=list0.par.sum
    println("result2==== "+result2)

    //化简：reduce
    //将非特定顺序的二元操作应用到所有元素
    val result3=list0.reduce((x,y) => x + y)
    println("result3==== "+result3)

    //按照特定的顺序
    val result4 = list0.reduceLeft(_+_)
    val result5= list0.reduceRight(_+_)
    println("result4==== "+result4)
    println("result5==== "+result5)

    //折叠：有初始值（无特定顺序）
    val result6 = list0.fold(100)((x,y)=>x+y)
    println("result6==== "+result6)

    //折叠：有初始值（有特定顺序）
    val result7 = list0.foldLeft(100)((x,y)=>x+y)
    println("result7==== "+result7)

    //聚合
    val  list10= List(List(1, 2, 3), List(4, 5, 6), List(7,8), List(9,0))
    val  result8 = list10.par.aggregate(10)(_+_.sum,_+_)
    println("result8==== "+result8)

    //获取到参与并行计算的线程
    println(list10.par.collect{
      case _=>Thread.currentThread().getName
    }.distinct)

    val l1 = List(5,6,4,7)
    val l2 = List(1,2,3,4)
    //求并集
    val r1=l1.union(l2)
    println("r1=== "+r1)

    //求交集
    val r2=l1.intersect(l2)
    println("r1=== "+r2)

    //求差集
    val r3=l1.diff(l2)
    println("r3=== "+r3)




  }
}
