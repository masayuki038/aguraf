package net.wrap_trap.aguraf

object Helper {
  def using[A, R <: {def close()}](r: R)(f: R => A): A = {
    try {
      f(r)
    } finally {
      try {r.close()} catch {case ignore: Exception => {}}
    }
  }
}