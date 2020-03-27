package com.coke.nsrV3

import org.specs._
import org.specs.runner.{ConsoleRunner, JUnit4}

class MySpecSparkBasic extends JUnit4(MySpec)
//class MySpecSuite extends ScalaTestSuite(MySpec)
object MySpecRunner extends ConsoleRunner(MySpec)

object MySpec extends Specification {
  "This wonderful system" should {
    "save the world" in {
      val list = Nil
      list must beEmpty
    }
  }
}
