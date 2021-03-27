package Generator.DatasetJob.imdb_kaggle.table

import Generator.DatasetJob.AbstractTableOperatorManager
import Generator.DatasetJob.utils.getElementBySeed

/**
  * Created by researchuser7 on 2020-06-24.
  */
case class NameBasicsOperatorManager() extends AbstractTableOperatorManager {
  override val tableName: String = "name.basics"
  override val typeSchema: String = "(String, String, Int, Int, String, String)"
  override val fields: Map[String, String] = Map(
    "nconst" -> "1",
    "primaryName" -> "2",
    "birthYear" -> "3",
    "deathYear" -> "4",
    "primaryProfession" -> "5",
    "knownForTitles" -> "6"
  )
  override val joinFieldTable: Map[String, Map[String, String]] = Map("nconst"-> Map("title.principals"-> "nconst"))

  override val filterFieldValue: Map[String, Map[String, Seq[String]]] = Map(
    "birthYear"-> Map("selectivity"-> Seq("0.95"), "values"-> Seq( "1869.0")),
    "deathYear"-> Map("selectivity"-> Seq("0.99"), "values"-> Seq("1991.0")),
    "primaryProfession"-> Map("selectivity"-> Seq("0.2334343388368392", "0.13995941930725725", "0.015487350353031844", "0.03521060914082987", "0.007280181220884887", "0.020137015444924335", "0.01983653213842992", "0.06051680245395387", "0.0043183920291808625", "0.002497265478339973", "0.028078610057726158", "0.024074088185568936", "0.011674260443442512", "0.006064346265511498", "0.07986389074188072", "0.03653858471332883", "0.014322488405442806", "5.14878866508592e-07", "0.002512299941242024", "0.00032076953383485283", "0.006030673187641836", "0.017332058355958826", "0.01740362651840352", "0.10420520105916761", "0.018660033928457786", "0.0", "0.08735630503154045", "6.178546398103104e-07", "0.006468114272627536", "0.016329486227093296", "0.0003360099482835071", "0.005979391252537581", "0.004366996594179274", "0.030538495330357608", "0.01367435888828179", "0.005551732866015544", "0.006967134870047663", "0.0011667155115084695", "0.0074658465401478856", "0.017380353993637333", "0.06625636313047174"), "values"-> Seq("actor", "actress", "animation_department", "art_department", "art_director", "assistant", "assistant_director", "camera_department", "casting_department", "casting_director", "cinematographer", "composer", "costume_department", "costume_designer", "director", "editor", "editorial_department", "electrical_department", "executive", "legal", "location_management", "make_up_department", "manager", "miscellaneous", "music_department", "nan", "producer", "production_department", "production_designer", "production_manager", "publicist", "script_department", "set_decorator", "sound_department", "soundtrack", "special_effects", "stunts", "talent_agent", "transportation_department", "visual_effects", "writer")))

  override val groupFields: Map[String, String] = Map("primaryProfession"-> "19674")

  override def dataSourceCode(outVarName: String): String = buildDataSourceCode(typeSchema, tableName + ".csv", outVarName, delimiter = ";")

  def filterCode(inVarName: String, outVarName: String, fieldSeed: Int = 0, valueSeed: Int = 0): (String, Double) = {
    val field = filterFields(fieldSeed % filterFields.size)

    var filterField = ""
    var filterValue = ""
    var filterOp = ""

    field match {
      case "birthYear" =>
        filterField = fields(field)
        filterValue = getElementBySeed(filterFieldValue(field)("values"), valueSeed).toString
        filterOp = "<="

      case "deathYear" =>
        filterField = fields(field)
        filterValue = getElementBySeed(filterFieldValue(field)("values"), valueSeed).toString
        filterOp = "<="

      case "primaryProfession" =>
        filterField = fields(field)
        filterValue = s""" "${getElementBySeed(filterFieldValue(field)("values"), valueSeed).toString}" """
        filterOp = "contains"


      case _ => throw new Exception(s"Can not find field with name '$field' in filter operator generation.")
    }

    (
      buildFilterCode(inVarName, outVarName, filterField, filterValue, filterOp),
      getElementBySeed(filterFieldValue(field)("selectivity"), valueSeed).toString.toDouble
    )

  }
}
