package util

class CyclicBuffer(size: Int) {
  val arr = Array.fill(size)(-1)
  val ind = Array.fill(size)(-1)
  var version = -1

  var ptr = 0
  var iter = -1

  def push(v: Int, i: Int): Unit = {
    ptr = (ptr - 1 + size) % size
    arr(ptr) = v
    ind(ptr) = i
  }

  def reset(): Unit = {
    ptr = 0
    iter = -1
    ind(ptr) = -1
    arr(ptr) = -1
  }

  def resetIter(): Unit = {
    iter = ptr
  }

  def headInd(): Int = {
    if (iter == -1) {
      -1
    } else {
      ind(iter)
    }
  }

  def headVal(): Int = {
    if (iter == -1) {
      -1
    } else {
      arr(iter)
    }
  }

  def next(): Unit = {
    if (iter == (ptr - 1 + size) % size) {
      iter = -1
    } else {
      iter = (iter + 1) % size
    }
  }

  def checkVersion(v: Int): Unit = {
    if (v != version) {
      version = v
      reset()
    }
  }
}
