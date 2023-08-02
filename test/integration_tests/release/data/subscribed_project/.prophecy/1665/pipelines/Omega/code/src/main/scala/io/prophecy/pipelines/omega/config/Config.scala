package io.prophecy.pipelines.omega.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(a: String = "a", b: String = "b", c: C = C())
    extends ConfigBase

object C {

  implicit val confHint: ProductHint[C] =
    ProductHint[C](ConfigFieldMapping(CamelCase, CamelCase))

}

case class C(d: List[D] = List(D(t = "a"), D(t = "q")), e: E = E())

object D {

  implicit val confHint: ProductHint[D] =
    ProductHint[D](ConfigFieldMapping(CamelCase, CamelCase))

}

case class D(t: String)

object E {

  implicit val confHint: ProductHint[E] =
    ProductHint[E](ConfigFieldMapping(CamelCase, CamelCase))

}

case class E(k: String = "a")
