import java.util
import org.apache.flink.cep.CEP
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

class Event(var id: Int, var name: String, var foo: Double) {

}

object FlinkCEPStarter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val input = env.fromElements(
      new Event(1, "barfoo", 1.0),
      new Event(2, "start", 2.0),
      new Event(3, "foobar", 3.0),
      new Event(4, "foo", 4.0),
      new Event(5, "middle", 5.0),
      new Event(42, "42", 42.0),
      new Event(8, "end", 1.0)
    )

    val pattern: Pattern[Event, Event] = Pattern.begin[Event]("start")
      .where(new SimpleCondition[Event] {
        override def filter(e: Event): Boolean = {
          e.name.equals("start")
        }
      })
      .followedByAny("middle").subtype[Event](classOf[Event])
      .where(new SimpleCondition[Event] {
        override def filter(e: Event): Boolean = {
          e.name.equals("middle")
        }
      })
      .followedByAny("end")
      .where(new SimpleCondition[Event] {
        override def filter(e: Event): Boolean = {
          e.name.equals("end")
        }
      })

    val patternStream = CEP.pattern(input, pattern)
    val result = patternStream.process(
      new PatternProcessFunction[Event, String] {
        // 此处因为数据放在一个map里面了, 丧失了先后顺序需要特别注意
        override def processMatch(matchResult: util.Map[String, util.List[Event]],
                                  ctx: PatternProcessFunction.Context, out: Collector[String]): Unit = {
          import scala.collection.JavaConverters._
          val info = matchResult.asScala.map { case (k, v) =>
            (k, v.asScala.mkString(","))
          }.mkString(";")

          out.collect(info)
        }
      }
    )
    result.print()
    env.execute("cep demo")
  }
}
