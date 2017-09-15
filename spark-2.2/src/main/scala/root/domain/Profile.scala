package root.domain

/**
  * User: Constantine Solovev
  * Created: 15.09.17  9:39
  */


case class Segments(customerId: String, userId: String, segments: Seq[String])

case class Profile(id: String, system: Array[Attr], attributes: Seq[Int])

case class Attr(key: String, value: String)
