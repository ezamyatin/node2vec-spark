package util

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import scala.collection.parallel.ForkJoinTaskSupport


object ParItr {


  def foreach[A, B](i: Iterator[A], cpus: Int)(f: A => Unit): Unit = {
    val inQueue = new LinkedBlockingQueue[A](cpus * 5)
    val totalCounter = new AtomicLong(0)

    val consumers = Array.fill(cpus)(new Thread(new Runnable {
      override def run(): Unit = {
        try {
          while (true) {
            val obj = inQueue.take()
            f(obj)
            totalCounter.decrementAndGet()
          }
        } catch {
          case e: InterruptedException =>
        }
      }
    }))

    consumers.foreach(_.start())
    i.foreach{e =>
      inQueue.put(e)
      totalCounter.incrementAndGet()
    }

    while (totalCounter.get() > 0) {
      Thread.sleep(10)
    }

    consumers.foreach(_.interrupt())
    consumers.foreach(_.join())

    assert(totalCounter.get() == 0)
    assert(!i.hasNext)
  }

  def map[A, B](i: Iterator[A], cpus: Int)(f: A => B): Iterator[B] = {
    val inQueue = new LinkedBlockingQueue[A](cpus * 5)
    val outQueue = new LinkedBlockingQueue[B](cpus * 10)
    val totalCounter = new AtomicLong(0)

    val consumers = Array.fill(cpus)(new Thread(new Runnable {
      override def run(): Unit = {
        try {
          while (true) {
            val obj = inQueue.take()
            outQueue.put(f(obj))
            totalCounter.decrementAndGet()
          }
        } catch {
          case e: InterruptedException =>
        }
      }
    }))

    consumers.foreach(_.start())
    val ib = i.buffered

    new Iterator[B] {
      var put = 0
      var taken = 0

      override def hasNext: Boolean = {
        val r = i.hasNext || put > taken
        if (!r) {
          consumers.foreach(_.interrupt())
          consumers.foreach(_.join())
        }
        r
      }

      override def next(): B = {
        while (ib.hasNext && inQueue.offer(ib.head)) {
          ib.next()
          put += 1
        }

        taken += 1
        outQueue.take()
      }
    }
  }
}