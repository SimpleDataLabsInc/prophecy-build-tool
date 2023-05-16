package io.prophecy.pipelines.automatedpbttruescala.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  c_string:  String = "string$$%^&*#@",
  c_int:     Int = 65530,
  c_boolean: Boolean = true
) extends ConfigBase
