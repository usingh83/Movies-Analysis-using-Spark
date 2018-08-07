package lab

//import org.apache.spark.SparkContext
import org.apache.spark._

object Ques3
{

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "Ques3")
    val Movies_Dataset = sc.textFile(args(0))
    val Rating_Dataset = sc.textFile(args(1))    
    val Tags_Dataset = sc.textFile(args(2))
    val Movies = Movies_Dataset.map(record => (record.split(",")(0),record)).filter(t=> t._1!="movieId")
    val Rating = Rating_Dataset.map(record => (record.split(",")(1),record)).filter(t=> t._1!="movieId")    
    val Tags = Tags_Dataset.map(record => (record.split(",")(1),record)).filter(t=> t._1!="movieId")
    val Movies_List = Movies.map(t => (t._2.split(",")(0),t._2.split(",")(1)))
    val Rating_List = Rating.map(t => (t._2.split(",")(1),t._2.split(",")(2).toFloat))    
    val ActionTags_List = Tags.map(t => (t._2.split(",")(1),t._2.split(",")(2))).filter(x=>x._2=="action")
    val Movies_Rating=Movies_List.join(Rating_List)
    val ActionTagMovies_Rating=Rating_List.join(ActionTags_List).join(Movies_List)
    //    val ActionTagMovies_Rating=Rating_List.join(ActionTags_List).join(Movies_List)

    val Dataset_Movies_1=Movies.map(t=>(t._2.split(",")(0),t._2.split(",")(1),t._2.split(",")(2).split("\\|").toList))
    val Thriller_Genre_Movie=Dataset_Movies_1.flatMap(r=> r._3.map(x=> (r._1,(r._2,x)))).filter(x=> x._2._2=="Thriller")
    val ThrilGenreActionTagMovies_Rating=Rating_List.join(ActionTags_List).join(Thriller_Genre_Movie)
    val SumAndCount=Movies_Rating.map(x=>(x._2._1,x._2._2)).aggregateByKey((0.0, 0))(
        (acc, rating) => ( acc._1+rating, acc._2+1),
        (total1,total2) => (total1._1+total2._1,total1._2+total2._2)
        )
    val ActionTagSumAndCount=ActionTagMovies_Rating.map(x=>(x._2._2,x._2._1._1)).aggregateByKey((0.0, 0))(
        (acc, rating) => ( acc._1+rating, acc._2+1),
        (total1,total2) => (total1._1+total2._1,total1._2+total2._2)
        )
    val ThrilGenreActionTagSumAndCount=ThrilGenreActionTagMovies_Rating.map(x=>(x._2._2._1,x._2._1._1)).aggregateByKey((0.0, 0))(
        (acc, rating) => ( acc._1+rating, acc._2+1),
        (total1,total2) => (total1._1+total2._1,total1._2+total2._2)
        )
    val avgRatingByMovie=SumAndCount.map(x=>(x._1,x._2._1/x._2._2))
    val ActionTagavgRatingByMovie=ActionTagSumAndCount.map(x=>(x._1,x._2._1/x._2._2))
    val ThrilGenreActionTagavgRatingByMovie=ThrilGenreActionTagSumAndCount.map(x=>(x._1,x._2._1/x._2._2))
    avgRatingByMovie.saveAsTextFile(args(3))
    ActionTagavgRatingByMovie.saveAsTextFile(args(5))
    ThrilGenreActionTagavgRatingByMovie.saveAsTextFile(args(6))
    val Bottom10avgRating=sc.parallelize(avgRatingByMovie.sortBy(_._2,true).take(10))
    Bottom10avgRating.saveAsTextFile(args(4))
    
  }
}