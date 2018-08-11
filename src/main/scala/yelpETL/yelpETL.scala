package yelpETL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark


object yelpETL {

  val sparkConf =
    new SparkConf().setAppName("abc app").setMaster("local")
  sparkConf.getExecutorEnv
  val sparkSession: SparkSession =
    SparkSession.builder.config(sparkConf).getOrCreate()

  def main(args: Array[String]): Unit = {

    val yelpBus = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/home/yelp_business.csv")

    val yelpBust0 = yelpBus
      .withColumn("business_idLength",length(trim(yelpBus("business_id"))))
      .withColumn("postal_codeLength",length(trim(yelpBus("postal_code"))))

    val yelpBust1 = yelpBust0
      .filter(col("business_idLength").equalTo(22) && col("postal_codeLength").equalTo(5))

    val yelpBust2 = yelpBust1
      .filter(col("latitude") rlike "^-?\\d*\\.\\d*$" )
      .filter( col("longitude") rlike "^-?\\d*\\.\\d*$" )
      .filter( col("stars") rlike "^\\d\\.\\d$" )
      .filter( col("review_count") rlike "^\\d*$" )
      .filter( col("is_open") rlike "^0\\.0|1\\.0$")

    val yelpBsut3 = yelpBust2.repartition(1)

    yelpBsut3.write.format("csv").save("/vol/yelp_business.csv")

    val yelpBA = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/home/yelp_business_attributes.csv")

    val yelpBAt0 = yelpBA
      .withColumn("business_idLength",length(trim(yelpBA("business_id"))))

    val yelpBAt1 = yelpBAt0
      .filter(col("business_idLength").equalTo(22))
      .filter(col("AcceptsInsurance") =!= "Na" ||  col("ByAppointmentOnly")=!= "Na" ||  col("BusinessAcceptsCreditCards")=!= "Na" ||  col("BusinessParking_garage") =!= "Na" ||  col("BusinessParking_street") =!= "Na" ||  col("BusinessParking_validated") =!= "Na" ||  col("BusinessParking_lot") =!= "Na" ||  col("BusinessParking_valet") =!= "Na" ||  col("HairSpecializesIn_coloring") =!= "Na" ||  col("HairSpecializesIn_africanamerican") =!= "Na" ||  col("HairSpecializesIn_curly") =!= "Na" ||  col("HairSpecializesIn_perms") =!= "Na" ||  col("HairSpecializesIn_kids") =!= "Na" ||  col("HairSpecializesIn_extensions") =!= "Na" ||  col("HairSpecializesIn_asian") =!= "Na" ||  col("HairSpecializesIn_straightperms") =!= "Na" ||  col("RestaurantsPriceRange2") =!= "Na" ||  col("GoodForKids") =!= "Na" ||  col("WheelchairAccessible") =!= "Na" ||  col("BikeParking") =!= "Na" ||  col("Alcohol") =!= "Na" ||  col("HasTV") =!= "Na" ||  col("NoiseLevel") =!= "Na" ||  col("RestaurantsAttire") =!= "Na" ||  col("Music_dj")=!= "Na" ||  col("Music_background_music")=!= "Na" ||  col("Music_no_music")=!= "Na" ||  col("Music_karaoke") =!= "Na" ||  col("Music_live") =!= "Na" ||  col("Music_video") =!= "Na" ||  col("Music_jukebox") =!= "Na" ||  col("Ambience_romantic") =!= "Na" ||  col("Ambience_intimate") =!= "Na" ||  col("Ambience_classy") =!= "Na" ||  col("Ambience_hipster") =!= "Na" ||  col("Ambience_divey") =!= "Na" ||  col("Ambience_touristy") =!= "Na" ||  col("Ambience_trendy") =!= "Na" ||  col("Ambience_upscale") =!= "Na" ||  col("Ambience_casual") =!= "Na" ||  col("RestaurantsGoodForGroups") =!= "Na" ||  col("Caters") =!= "Na" ||  col("WiFi") =!= "Na" ||  col("RestaurantsReservations") =!= "Na" ||  col("RestaurantsTakeOut") =!= "Na" ||  col("HappyHour") =!= "Na" ||  col("GoodForDancing") =!= "Na" ||  col("RestaurantsTableService") =!= "Na" ||  col("OutdoorSeating")=!= "Na" ||  col("RestaurantsDelivery") =!= "Na" ||  col("BestNights_monday") =!= "Na" ||  col("BestNights_tuesday") =!= "Na" ||  col("BestNights_friday") =!= "Na" ||  col("BestNights_wednesday") =!= "Na" ||  col("BestNights_thursday") =!= "Na" ||  col("BestNights_sunday") =!= "Na" ||  col("BestNights_saturday") =!= "Na" ||  col("GoodForMeal_dessert") =!= "Na" ||  col("GoodForMeal_latenight") =!= "Na" ||  col("GoodForMeal_lunch") =!= "Na" ||  col("GoodForMeal_dinner") =!= "Na" ||  col("GoodForMeal_breakfast")=!= "Na" ||  col("GoodForMeal_brunch") =!= "Na" ||  col("CoatCheck") =!= "Na" ||  col("Smoking") =!= "Na" ||  col("DriveThru") =!= "Na" ||  col("DogsAllowed")=!= "Na" ||  col("BusinessAcceptsBitcoin") =!= "Na" ||  col("Open24Hours") =!= "Na" ||  col("BYOBCorkage") =!= "Na" ||  col("BYOB") =!= "Na" ||  col("Corkage") =!= "Na" ||  col("DietaryRestrictions_dairy-free") =!= "Na" ||  col("DietaryRestrictions_gluten-free") =!= "Na" ||  col("DietaryRestrictions_vegan") =!= "Na" ||  col("DietaryRestrictions_kosher") =!= "Na" ||  col("DietaryRestrictions_halal") =!= "Na" ||  col("DietaryRestrictions_soy-free") =!= "Na" ||  col("DietaryRestrictions_vegetarian") =!= "Na" ||  col("AgesAllowed") =!= "Na" ||  col("RestaurantsCounterService") =!= "Na")

    val yelpBAt2 = yelpBAt1.repartition(1)

    yelpBAt2.write.format("csv").save("/vol/yelp_business_attributes.csv")

    val yelpBH = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/home/yelp_business_hours.csv")

    val yelpBHt0 = yelpBH.withColumn("business_idLength",length(trim(yelpBH("business_id"))))

    val yelpBHt1 = yelpBHt0
      .filter(col("business_idLength").equalTo(22))

    val yelpBHt2 = yelpBHt1.repartition(1)

    yelpBHt2.write.format("csv").save("/vol/yelp_business_hours.csv")

    val yelpC = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/home/yelp_checkin.csv")

    val yelpCt0 = yelpC
      .withColumn("business_idLength",length(trim(yelpC("business_id"))))
      .withColumn("weekdayLength",length(trim(yelpC("weekday"))))

    val yelpCt1 = yelpCt0
      .filter(col("business_idLength").equalTo(22) && col("weekdayLength").equalTo(3))
      .filter(col("hour") rlike "([0-9]|1[012]):[0-5][0-9]")
      .filter(col("checkins ")rlike "[0-9][0-9]|[0-9]")

    val yelpCt2 = yelpCt1.repartition(2)

    yelpCt2.write.format("csv").save("/vol/yelp_checkin.csv")

    val yelpR = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/home/yelp_review.csv")

    val yelpRt0 = yelpR
      .withColumn("business_idLength",length(trim(yelpR("business_id"))))
      .withColumn("review_idLength",length(trim(yelpR("review_id"))))
      .withColumn("user_idLength",length(trim(yelpR("user_id"))))

    val yelpRt1 = yelpRt0
      .filter(col("business_idLength").equalTo(22) && col("user_idLength").equalTo(22) && col("review_idLength").equalTo(22))
      .filter(col("stars") rlike "\\d")
      .filter(col("date") rlike "\\d{4}-\\d{2}-\\d{2}")
      .filter(col("useful") rlike "\\d")
      .filter(col("funny") rlike "\\d")
      .filter(col("cool") rlike "\\d")

    val yelpRt2 = yelpRt1.repartition(1)

    yelpRt2.write.format("csv").save("/vol/yelp_review.csv")

    val yelpT = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/home/yelp_tip.csv")

    val yelpTt0 = yelpT
      .withColumn("business_idLength",length(trim(yelpT("business_id"))))
      .withColumn("user_idLength",length(trim(yelpT("user_id"))))

    val yelpTt1 = yelpTt0
      .filter(col("business_idLength").equalTo(22) && col("user_idLength").equalTo(22))
      .filter(col("date") rlike "\\d{4}-\\d{2}-\\d{2}")
      .filter(col("likes") rlike "\\d|\\d\\d")

    val yelpTt2 = yelpTt1.repartition(1)

    yelpTt2.write.format("csv").save("/vol/yelp_tip.csv")

    val yelpU = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/home/yelp_user.csv")

    val yelpUt0 = yelpU
      .withColumn("user_idLength",length(trim(yelpU("user_id"))))

    val yelpUt1 = yelpUt0
      .filter(col("user_idLength").equalTo(22))
      .filter(col("review_count") rlike "\\d\\d|\\d")
      .filter(col("yelping_since") rlike "\\d{4}-\\d{2}-\\d{2}")
      .filter(col("useful") rlike "\\d\\d|\\d")
      .filter(col("funny") rlike "\\d\\d|\\d")
      .filter(col("cool") rlike "\\d\\d|\\d")
      .filter(col("fans") rlike "\\d\\d|\\d")
      .filter(col("average_stars") rlike "\\d\\.\\d|\\d")
      .filter(col("compliment_hot") rlike "\\d\\d|\\d")
      .filter(col("compliment_more") rlike "\\d\\d|\\d")
      .filter(col("compliment_profile") rlike "\\d\\d|\\d")
      .filter(col("compliment_cute") rlike "\\d\\d|\\d")
      .filter(col("compliment_list") rlike "\\d\\d|\\d")
      .filter(col("compliment_note") rlike "\\d\\d|\\d")
      .filter(col("compliment_plain") rlike "\\d\\d|\\d")
      .filter(col("compliment_cool") rlike "\\d\\d|\\d")
      .filter(col("compliment_funny") rlike "\\d\\d|\\d")
      .filter(col("compliment_writer") rlike "\\d\\d|\\d")
      .filter(col("compliment_photos") rlike "\\d\\d|\\d")

    val yelpUt2 = yelpUt1.repartition(1)

    yelpUt2.write.format("csv").save("/vol/yelp_user.csv")

    }

  }
