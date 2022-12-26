package util

import scala.collection.parallel.ForkJoinTaskSupport


object ParItr {

  def foreach[A](i: Iterator[A], cpus: Int)(f: A => Unit): Unit = {
    val support = new ForkJoinTaskSupport(new java.util.concurrent.ForkJoinPool(cpus))
    val parr = (0 until cpus).toArray.par
    parr.tasksupport = support
    parr.foreach{_ =>
      var x: A = null.asInstanceOf[A]

      while (synchronized{
        if (i.hasNext) {
          x = i.next()
          true
        } else {
          x = null.asInstanceOf[A]
          false
        }
      }) {
        f(x)
      }
    }
  }

  def map[A, B](i: Iterator[A], cpus: Int, batch: Int)(f: A => B): Iterator[B] = {
    val support = new ForkJoinTaskSupport(new java.util.concurrent.ForkJoinPool(cpus))
    i.grouped(batch).flatMap{arr =>
      val parr = arr.par
      parr.tasksupport = support
      parr.map(f)
    }
  }
}